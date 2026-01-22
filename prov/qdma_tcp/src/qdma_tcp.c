/* qdma_tcp.c - Hybrid QDMA/TCP Libfabric Provider
 *
 * A libfabric provider that uses Xilinx QDMA for fast-path message transfers
 * and falls back to TCP sockets when QDMA is unavailable.
 *
 * ============================================================================
 * SETUP REQUIREMENTS:
 * ============================================================================
 *
 * 1. HARDWARE & DRIVER:
 *    - Xilinx FPGA with QDMA IP core
 *    - QDMA kernel driver loaded (qdma-pf.ko or qdma-vf.ko)
 *      Verify: lsmod | grep qdma
 *
 * 2. PRE-CONFIGURE QDMA QUEUES (using dma-ctl tool):
 *    Before running any libfabric application, create and start queues:
 *
 *    # Example: Create streaming mode (ST) H2C queue on device qdma01000
 *    dma-ctl qdma01000 q add idx 0 mode st dir h2c
 *    dma-ctl qdma01000 q start idx 0 dir h2c
 *
 *    # For bidirectional (H2C + C2H), also add C2H queue:
 *    dma-ctl qdma01000 q add idx 0 mode st dir c2h
 *    dma-ctl qdma01000 q start idx 0 dir c2h
 *
 *    This creates character device nodes:
 *      /dev/qdma01000-ST-0  (H2C queue - host-to-card)
 *      /dev/qdma01000-ST-0  (C2H queue - card-to-host)
 *
 * 3. SET ENVIRONMENT VARIABLE:
 *    export QDMA_QUEUE_DEV=/dev/qdma01000-ST-0
 *
 * 4. RUN YOUR LIBFABRIC APPLICATION:
 *    The provider will automatically:
 *    - Open the queue device for QDMA fast-path if QDMA_QUEUE_DEV is set
 *    - Fall back to TCP sockets if QDMA_QUEUE_DEV is unset or open fails
 *
 * ============================================================================
 * RUNTIME BEHAVIOR:
 * ============================================================================
 *
 * - Send operations: write() syscall to /dev/qdmaXXXXX (H2C DMA transfer)
 * - Receive operations: read() syscall from /dev/qdmaXXXXX (C2H DMA transfer)
 * - Fallback: Automatic TCP socket fallback if QDMA unavailable
 * - Queue lifecycle: Managed externally via dma-ctl (not by libfabric)
 * ============================================================================
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>

#include "qdma_tcp.h"

/* Provider init macro (built-in vs dlopen plugin) */
#include <ofi_prov.h>

/* libqdma types (qdma_request, qdma_queue_conf, etc) */
#if defined(__linux__)
#include <sys/uio.h>
#include <fcntl.h>
#include <unistd.h>
#endif

/* External util TCP provider symbol */
extern struct util_prov xnet_util_prov;

/* ============================================================================
 * CALL FLOW:
 * 1. fi_prov_ini()       - Provider initialization (library load)
 * 2. fi_getinfo()        - Query provider capabilities
 * 3. fi_fabric()         - Open fabric
 * 4. fi_domain()         - Open domain (CQ/AV/MR operations here)
 * 5. fi_endpoint()       - Open endpoint (MSG operations here)
 * 6. fi_send/fi_recv()   - Data operations
 * 7. fi_close()          - Cleanup (endpoint → domain → fabric)
 * ============================================================================ */

/* Helper: map errno to libfabric negative codes (comprehensive) */
static inline int errno_to_fi(int err)
{
    switch (err)
    {
    case 0:
        return 0;
    case ENOMEM:
        return -FI_ENOMEM;
    case ENODEV:
    case ENOENT:
        return -FI_ENODEV;
    case EINVAL:
        return -FI_EINVAL;
    case EACCES:
    case EPERM:
        return -FI_EACCES;
    case EAGAIN:
        return -FI_EAGAIN;
    case ECONNREFUSED:
    case ECONNRESET:
    case ECONNABORTED:
        return -FI_ENOTCONN;
    case ETIMEDOUT:
        return -FI_ETIMEDOUT;
    case ENOSPC:
    case EDQUOT:
        return -FI_ENOSPC;
    case EMFILE:
    case ENFILE:
        return -FI_EMFILE;
    case EBUSY:
        return -FI_EBUSY;
    case ECANCELED:
        return -FI_ECANCELED;
    default:
        return -FI_EIO;
    }
}

/* ---------------------- Async worker (lightweight) ---------------------- */
#if defined(__linux__)
#include <pthread.h>

/* CQ helper is referenced by async worker code below */
static void qdma_tcp_cq_enqueue(struct qdma_tcp_cq *cq, void *op_context);

/* Async I/O request for background write operations */
struct qdma_io_req
{
    struct qdma_io_req *next;
    struct qdma_tcp_ep *ep;
    struct iovec *iov;
    int iovcnt;
    size_t total;
    int op;    /* 0 = TX, 1 = RX */
    void *ctx; /* user completion context (fi_context or similar) */
};

struct qdma_async_mgr
{
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct qdma_io_req *head;
    struct qdma_io_req *tail;
    int stop;
    int inflight;
    int max_inflight;
} qdma_async = {
    .thread = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .cond = PTHREAD_COND_INITIALIZER,
    .head = NULL,
    .tail = NULL,
    .stop = 0,
    .inflight = 0,
    .max_inflight = 1024,
};

static void qdma_async_post_cq(struct qdma_io_req *req, ssize_t res)
{
    struct qdma_tcp_ep *ep = req->ep;
    if (!ep)
        return;

    (void)res;
    if (ep->tx_cq)
        qdma_tcp_cq_enqueue(ep->tx_cq, req->ctx ? req->ctx : ep);
}

static void *qdma_async_worker(void *arg)
{
    (void)arg;
    while (1)
    {
        pthread_mutex_lock(&qdma_async.lock);
        while (!qdma_async.head && !qdma_async.stop)
            pthread_cond_wait(&qdma_async.cond, &qdma_async.lock);
        if (qdma_async.stop && !qdma_async.head)
        {
            pthread_mutex_unlock(&qdma_async.lock);
            break;
        }
        struct qdma_io_req *req = qdma_async.head;
        if (req)
        {
            qdma_async.head = req->next;
            if (!qdma_async.head)
                qdma_async.tail = NULL;
        }
        pthread_mutex_unlock(&qdma_async.lock);

        if (!req)
            continue;

        ssize_t total = 0;
        int fd = req->ep ? req->ep->qdev_fd : -1;
        if (fd > 0)
        {
            int iov_idx = 0;
            struct iovec *iov = req->iov;
            int iovcnt = req->iovcnt;
            while (iovcnt > 0)
            {
                ssize_t w = writev(fd, iov + iov_idx, iovcnt);
                if (w > 0)
                {
                    total += w;
                    size_t skip = (size_t)w;
                    while (iov_idx < req->iovcnt && skip > 0)
                    {
                        if ((size_t)req->iov[iov_idx].iov_len <= skip)
                        {
                            skip -= req->iov[iov_idx].iov_len;
                            iov_idx++;
                            iovcnt--;
                        }
                        else
                        {
                            req->iov[iov_idx].iov_base = (char *)req->iov[iov_idx].iov_base + skip;
                            req->iov[iov_idx].iov_len -= skip;
                            skip = 0;
                        }
                    }
                    continue;
                }
                if (w == 0)
                    break;
                if (errno == EINTR)
                    continue;
                total = -errno;
                break;
            }
        }

        qdma_async_post_cq(req, total);

        struct qdma_tcp_ep *ep = req->ep;

        if (req->iov)
            free(req->iov);
        free(req);

        pthread_mutex_lock(&qdma_async.lock);
        qdma_async.inflight--;
        if (ep)
        {
            if (ep->async_inflight > 0)
                ep->async_inflight--;
        }
        pthread_cond_broadcast(&qdma_async.cond);
        pthread_mutex_unlock(&qdma_async.lock);
    }

    return NULL;
}

static int qdma_async_init(void)
{
    int rc;
    qdma_async.stop = 0;
    qdma_async.head = qdma_async.tail = NULL;
    qdma_async.inflight = 0;
    pthread_mutex_init(&qdma_async.lock, NULL);
    pthread_cond_init(&qdma_async.cond, NULL);
    rc = pthread_create(&qdma_async.thread, NULL, qdma_async_worker, NULL);
    if (rc != 0)
        return -1;
    return 0;
}

static void qdma_async_fini(void)
{
    pthread_mutex_lock(&qdma_async.lock);
    qdma_async.stop = 1;
    pthread_cond_signal(&qdma_async.cond);
    pthread_mutex_unlock(&qdma_async.lock);
    if (qdma_async.thread)
        pthread_join(qdma_async.thread, NULL);

    pthread_mutex_lock(&qdma_async.lock);
    struct qdma_io_req *r = qdma_async.head;
    while (r)
    {
        struct qdma_io_req *n = r->next;
        if (r->iov)
            free(r->iov);
        free(r);
        r = n;
    }
    qdma_async.head = qdma_async.tail = NULL;
    pthread_mutex_unlock(&qdma_async.lock);
}

static int qdma_async_submit_tx(struct qdma_tcp_ep *ep, struct iovec *iov, int iovcnt, size_t total, void *ctx)
{
    if (!ep || !iov || iovcnt <= 0)
        return -FI_EINVAL;

    pthread_mutex_lock(&qdma_async.lock);
    if (ep->closing)
    {
        pthread_mutex_unlock(&qdma_async.lock);
        return -FI_ECANCELED;
    }
    if (qdma_async.inflight >= qdma_async.max_inflight)
    {
        pthread_mutex_unlock(&qdma_async.lock);
        return -FI_EAGAIN;
    }
    /* reserve slot and increment per-ep counter */
    qdma_async.inflight++;
    ep->async_inflight++;
    pthread_mutex_unlock(&qdma_async.lock);

    struct qdma_io_req *req = calloc(1, sizeof(*req));
    if (!req)
    {
        pthread_mutex_lock(&qdma_async.lock);
        qdma_async.inflight--;
        if (ep->async_inflight > 0)
            ep->async_inflight--;
        pthread_mutex_unlock(&qdma_async.lock);
        return -FI_ENOMEM;
    }

    req->ep = ep;
    req->iov = malloc(sizeof(struct iovec) * iovcnt);
    if (!req->iov)
    {
        free(req);
        pthread_mutex_lock(&qdma_async.lock);
        qdma_async.inflight--;
        if (ep->async_inflight > 0)
            ep->async_inflight--;
        pthread_mutex_unlock(&qdma_async.lock);
        return -FI_ENOMEM;
    }
    memcpy(req->iov, iov, sizeof(struct iovec) * iovcnt);
    req->iovcnt = iovcnt;
    req->total = total;
    req->op = 0; /* TX */
    req->ctx = ctx;
    req->next = NULL;

    pthread_mutex_lock(&qdma_async.lock);
    if (qdma_async.tail)
        qdma_async.tail->next = req;
    else
        qdma_async.head = req;
    qdma_async.tail = req;
    pthread_cond_signal(&qdma_async.cond);
    pthread_mutex_unlock(&qdma_async.lock);

    return 0;
}
#endif /* __linux__ */

/* ============================================================================
 * FORWARD DECLARATIONS
 * ============================================================================ */

static int qdma_tcp_getinfo(uint32_t version, const char *node, const char *service,
                            uint64_t flags, const struct fi_info *hints, struct fi_info **info);
static int qdma_tcp_fabric(struct fi_fabric_attr *attr, struct fid_fabric **fabric, void *context);
static void qdma_tcp_cleanup(void);

static int qdma_tcp_domain_open(struct fid_fabric *fabric_fid, struct fi_info *info,
                                struct fid_domain **domain_fid, void *context);
static int qdma_tcp_domain_close(struct fid *fid);

static struct fi_ops qdma_tcp_domain_fi_ops;

static int qdma_tcp_ep_open(struct fid_domain *domain, struct fi_info *info,
                            struct fid_ep **ep_fid, void *context);
static int qdma_tcp_ep_close(struct fid *fid);

static void qdma_tcp_fid_map_init_once(void);

/* ============================================================================
 * 1. PROVIDER INITIALIZATION (fi_prov_ini)
 * ============================================================================ */

static struct fi_provider qdma_tcp_provider = {
    .name = QDMA_TCP_PROVIDER_NAME,
    .version = FI_VERSION(1, 0),
    .fi_version = QDMA_TCP_FI_VERSION,
    .context = {0},
    .getinfo = NULL, /* set in fi_prov_ini */
    .fabric = NULL,  /* set in fi_prov_ini */
    .cleanup = NULL, /* set in fi_prov_ini */
};

/* Provider entry point - called by libfabric when loading the provider */
QDMA_TCP_INI
{
    qdma_tcp_provider.getinfo = qdma_tcp_getinfo;
    qdma_tcp_provider.fabric = qdma_tcp_fabric;
    qdma_tcp_provider.cleanup = qdma_tcp_cleanup;
    qdma_tcp_fid_map_init_once();
#if defined(__linux__)
    (void)qdma_async_init();
#endif
    return &qdma_tcp_provider;
}

/* ============================================================================
 * 2. GETINFO (fi_getinfo)
 * ============================================================================ */

static int qdma_tcp_getinfo(uint32_t version, const char *node, const char *service,
                            uint64_t flags, const struct fi_info *hints, struct fi_info **info)
{
    int ret;
    struct fi_info *tmp_hints = NULL;
    const struct fi_info *hints_to_use = hints;
    const char *out_prov_name = QDMA_TCP_PROVIDER_NAME;

    if (!info)
        return -FI_EINVAL;

    /* If the app requested us explicitly (e.g. fi_info -p qdma_tcp), the hints
     * prov_name will likely be "qdma_tcp". The TCP/xnet util getinfo may treat
     * that as a mismatch and return -FI_ENODATA. Strip prov_name before
     * delegating and then rewrite prov_name on the returned info chain.
     */
    if (hints && hints->fabric_attr && hints->fabric_attr->prov_name &&
        !strncmp(hints->fabric_attr->prov_name, QDMA_TCP_PROVIDER_NAME,
                 strlen(QDMA_TCP_PROVIDER_NAME)) &&
        (hints->fabric_attr->prov_name[strlen(QDMA_TCP_PROVIDER_NAME)] == '\0' ||
         hints->fabric_attr->prov_name[strlen(QDMA_TCP_PROVIDER_NAME)] == ';'))
    {
        tmp_hints = fi_dupinfo(hints);
        if (!tmp_hints)
            return -FI_ENOMEM;
        if (tmp_hints->fabric_attr)
            tmp_hints->fabric_attr->prov_name = NULL;
        hints_to_use = tmp_hints;
    }

    /* Delegate to util TCP provider to build baseline info, then customize */
    ret = xnet_util_prov.prov->getinfo(version, node, service, flags, hints_to_use, info);

    if (tmp_hints)
        fi_freeinfo(tmp_hints);

    if (ret)
        return ret;

    /* qdma_tcp currently supports MSG endpoints (RXM can layer RDM on top).
     * Filter out any non-MSG endpoints returned by the underlying TCP util.
     */
    {
        struct fi_info *cur = *info;
        struct fi_info *prev = NULL;

        while (cur)
        {
            struct fi_info *next = cur->next;
            if (!cur->ep_attr || cur->ep_attr->type != FI_EP_MSG)
            {
                if (prev)
                    prev->next = next;
                else
                    *info = next;

                cur->next = NULL;
                fi_freeinfo(cur);
            }
            else
            {
                prev = cur;
            }
            cur = next;
        }

        if (!*info)
            return -FI_ENODATA;
    }

    /* Customize returned fi_info chain for hybrid QDMA/TCP provider */
    for (struct fi_info *cur = *info; cur; cur = cur->next)
    {
        if (cur->fabric_attr && cur->fabric_attr->prov_name)
            cur->fabric_attr->prov_name = (char *)out_prov_name;
        cur->caps |= FI_MSG;
        cur->mode |= FI_CONTEXT;
        cur->domain_attr->mr_mode = FI_MR_BASIC;
        cur->tx_attr->inject_size = 0;
    }
    return 0;
}

struct qdma_tcp_eq_map
{
    struct dlist_entry entry;
    fid_t xnet_fid;
    fid_t wrapper_fid;
};

struct qdma_tcp_eq
{
    struct fid_eq eq_fid;
    struct fid_eq *xnet_eq;
    ofi_mutex_t lock;
    struct dlist_entry map_list;
};

struct qdma_tcp_fid_map
{
    struct dlist_entry entry;
    fid_t xnet_fid;
    fid_t wrapper_fid;
};

static ofi_mutex_t qdma_tcp_fid_map_lock;
static struct dlist_entry qdma_tcp_fid_map_list;
static int qdma_tcp_fid_map_inited;

static void qdma_tcp_fid_map_init_once(void)
{
    if (qdma_tcp_fid_map_inited)
        return;
    ofi_mutex_init(&qdma_tcp_fid_map_lock);
    dlist_init(&qdma_tcp_fid_map_list);
    qdma_tcp_fid_map_inited = 1;
}

static fid_t qdma_tcp_fid_map_translate(fid_t in)
{
    struct dlist_entry *item;

    if (!qdma_tcp_fid_map_inited)
        return in;

    ofi_mutex_lock(&qdma_tcp_fid_map_lock);
    dlist_foreach(&qdma_tcp_fid_map_list, item)
    {
        struct qdma_tcp_fid_map *m = container_of(item, struct qdma_tcp_fid_map, entry);
        if (m->xnet_fid == in)
        {
            fid_t out = m->wrapper_fid;
            ofi_mutex_unlock(&qdma_tcp_fid_map_lock);
            return out;
        }
    }
    ofi_mutex_unlock(&qdma_tcp_fid_map_lock);
    return in;
}

static void qdma_tcp_fid_map_add(fid_t xnet_fid, fid_t wrapper_fid)
{
    struct qdma_tcp_fid_map *m;

    if (!xnet_fid || !wrapper_fid)
        return;
    qdma_tcp_fid_map_init_once();

    m = calloc(1, sizeof(*m));
    if (!m)
        return;
    m->xnet_fid = xnet_fid;
    m->wrapper_fid = wrapper_fid;
    ofi_mutex_lock(&qdma_tcp_fid_map_lock);
    dlist_insert_tail(&m->entry, &qdma_tcp_fid_map_list);
    ofi_mutex_unlock(&qdma_tcp_fid_map_lock);
}

static void qdma_tcp_fid_map_remove(fid_t xnet_fid)
{
    struct dlist_entry *item, *tmp;

    if (!qdma_tcp_fid_map_inited || !xnet_fid)
        return;

    ofi_mutex_lock(&qdma_tcp_fid_map_lock);
    dlist_foreach_safe(&qdma_tcp_fid_map_list, item, tmp)
    {
        struct qdma_tcp_fid_map *m = container_of(item, struct qdma_tcp_fid_map, entry);
        if (m->xnet_fid == xnet_fid)
        {
            dlist_remove(item);
            free(m);
            break;
        }
    }
    ofi_mutex_unlock(&qdma_tcp_fid_map_lock);
}

static fid_t qdma_tcp_eq_translate_fid(struct qdma_tcp_eq *eq, fid_t in)
{
    struct dlist_entry *item;

    if (!eq)
        return in;

    ofi_mutex_lock(&eq->lock);
    dlist_foreach(&eq->map_list, item)
    {
        struct qdma_tcp_eq_map *m = container_of(item, struct qdma_tcp_eq_map, entry);
        if (m->xnet_fid == in)
        {
            fid_t out = m->wrapper_fid;
            ofi_mutex_unlock(&eq->lock);
            return out;
        }
    }
    ofi_mutex_unlock(&eq->lock);
    return qdma_tcp_fid_map_translate(in);
}

static void qdma_tcp_eq_map_add(struct qdma_tcp_eq *eq, fid_t xnet_fid, fid_t wrapper_fid)
{
    struct qdma_tcp_eq_map *m;

    if (!eq || !xnet_fid || !wrapper_fid)
        return;

    m = calloc(1, sizeof(*m));
    if (!m)
        return;
    m->xnet_fid = xnet_fid;
    m->wrapper_fid = wrapper_fid;
    ofi_mutex_lock(&eq->lock);
    dlist_insert_tail(&m->entry, &eq->map_list);
    ofi_mutex_unlock(&eq->lock);
}

static ssize_t qdma_tcp_eq_read_common(struct qdma_tcp_eq *eq, uint32_t *event,
                                       void *buf, size_t len, uint64_t flags,
                                       int timeout, int is_sread)
{
    ssize_t ret;

    if (!eq || !eq->xnet_eq)
        return -FI_EINVAL;

    if (is_sread)
        ret = eq->xnet_eq->ops->sread(eq->xnet_eq, event, buf, len, timeout, flags);
    else
        ret = eq->xnet_eq->ops->read(eq->xnet_eq, event, buf, len, flags);

    if (ret > 0 && event && buf)
    {
        if ((*event == FI_CONNREQ || *event == FI_CONNECTED || *event == FI_SHUTDOWN) &&
            len >= sizeof(struct fi_eq_cm_entry))
        {
            struct fi_eq_cm_entry *entry = (struct fi_eq_cm_entry *)buf;
            entry->fid = qdma_tcp_eq_translate_fid(eq, entry->fid);
        }
    }

    return ret;
}

static ssize_t qdma_tcp_eq_read(struct fid_eq *eq_fid, uint32_t *event,
                                void *buf, size_t len, uint64_t flags)
{
    struct qdma_tcp_eq *eq = container_of(eq_fid, struct qdma_tcp_eq, eq_fid);
    return qdma_tcp_eq_read_common(eq, event, buf, len, flags, 0, 0);
}

static ssize_t qdma_tcp_eq_sread(struct fid_eq *eq_fid, uint32_t *event,
                                 void *buf, size_t len, int timeout, uint64_t flags)
{
    struct qdma_tcp_eq *eq = container_of(eq_fid, struct qdma_tcp_eq, eq_fid);
    return qdma_tcp_eq_read_common(eq, event, buf, len, flags, timeout, 1);
}

static ssize_t qdma_tcp_eq_readerr(struct fid_eq *eq_fid, struct fi_eq_err_entry *buf,
                                   uint64_t flags)
{
    struct qdma_tcp_eq *eq = container_of(eq_fid, struct qdma_tcp_eq, eq_fid);
    ssize_t ret;

    if (!eq || !eq->xnet_eq)
        return -FI_EINVAL;
    ret = eq->xnet_eq->ops->readerr(eq->xnet_eq, buf, flags);
    if (ret > 0 && buf)
        buf->fid = qdma_tcp_eq_translate_fid(eq, buf->fid);
    return ret;
}

static ssize_t qdma_tcp_eq_write(struct fid_eq *eq_fid, uint32_t event,
                                 const void *buf, size_t len, uint64_t flags)
{
    struct qdma_tcp_eq *eq = container_of(eq_fid, struct qdma_tcp_eq, eq_fid);
    if (!eq || !eq->xnet_eq)
        return -FI_EINVAL;
    return eq->xnet_eq->ops->write(eq->xnet_eq, event, buf, len, flags);
}

static const char *qdma_tcp_eq_strerror(struct fid_eq *eq_fid, int prov_errno,
                                        const void *err_data, char *buf, size_t len)
{
    struct qdma_tcp_eq *eq = container_of(eq_fid, struct qdma_tcp_eq, eq_fid);
    if (!eq || !eq->xnet_eq)
        return "qdma_tcp";
    return eq->xnet_eq->ops->strerror(eq->xnet_eq, prov_errno, err_data, buf, len);
}

static int qdma_tcp_eq_close(struct fid *fid)
{
    struct qdma_tcp_eq *eq = container_of(fid, struct qdma_tcp_eq, eq_fid.fid);
    struct dlist_entry *item, *tmp;

    if (!eq)
        return -FI_EINVAL;

    if (eq->xnet_eq)
    {
        fi_close(&eq->xnet_eq->fid);
        eq->xnet_eq = NULL;
    }

    ofi_mutex_lock(&eq->lock);
    dlist_foreach_safe(&eq->map_list, item, tmp)
    {
        struct qdma_tcp_eq_map *m = container_of(item, struct qdma_tcp_eq_map, entry);
        dlist_remove(item);
        free(m);
    }
    ofi_mutex_unlock(&eq->lock);
    ofi_mutex_destroy(&eq->lock);

    free(eq);
    return 0;
}

static struct fi_ops qdma_tcp_eq_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_eq_close,
    .bind = fi_no_bind,
    .control = fi_no_control,
    .ops_open = fi_no_ops_open,
};

static struct fi_ops_eq qdma_tcp_eq_ops = {
    .size = sizeof(struct fi_ops_eq),
    .read = qdma_tcp_eq_read,
    .readerr = qdma_tcp_eq_readerr,
    .write = qdma_tcp_eq_write,
    .sread = qdma_tcp_eq_sread,
    .strerror = qdma_tcp_eq_strerror,
};

/* ============================================================================
 * 3. FABRIC OPERATIONS (fi_fabric)
 * ============================================================================ */

static int qdma_tcp_fabric_close(struct fid *fid)
{
    struct qdma_tcp_fabric *fab = container_of(fid, struct qdma_tcp_fabric, util_fabric.fabric_fid.fid);
    if (fab->xnet_fabric)
        fi_close(&fab->xnet_fabric->fid);
    int ret = ofi_fabric_close(&fab->util_fabric);
    if (ret)
        return ret;
    free(fab);
    return 0;
}

static struct fi_ops qdma_tcp_fabric_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_fabric_close,
    .bind = fi_no_bind,
    .control = fi_no_control,
    .ops_open = fi_no_ops_open,
};

static int qdma_tcp_fabric_domain(struct fid_fabric *fabric, struct fi_info *info,
                                  struct fid_domain **domain, void *context)
{
    /* Return a qdma_tcp-owned domain wrapper. The wrapper will open a backing
     * xnet domain internally so that CQ/AV/CM remain functional.
     */
    return qdma_tcp_domain_open(fabric, info, domain, context);
}

static int qdma_tcp_fabric_eq_open(struct fid_fabric *fabric, struct fi_eq_attr *attr,
                                   struct fid_eq **eq_fid, void *context)
{
    struct qdma_tcp_fabric *fab = container_of(fabric, struct qdma_tcp_fabric, util_fabric.fabric_fid);
    struct qdma_tcp_eq *qeq;
    int ret;

    if (!fab->xnet_fabric || !eq_fid)
        return -FI_EINVAL;

    qeq = calloc(1, sizeof(*qeq));
    if (!qeq)
        return -FI_ENOMEM;

    ofi_mutex_init(&qeq->lock);
    dlist_init(&qeq->map_list);

    ret = fi_eq_open(fab->xnet_fabric, attr, &qeq->xnet_eq, context);
    if (ret)
    {
        ofi_mutex_destroy(&qeq->lock);
        free(qeq);
        return ret;
    }

    qeq->eq_fid.fid.fclass = FI_CLASS_EQ;
    qeq->eq_fid.fid.context = context;
    qeq->eq_fid.fid.ops = &qdma_tcp_eq_fi_ops;
    qeq->eq_fid.ops = &qdma_tcp_eq_ops;

    *eq_fid = &qeq->eq_fid;
    return 0;
}

static int qdma_tcp_fabric_trywait(struct fid_fabric *fabric, struct fid **fids, int count)
{
    struct qdma_tcp_fabric *fab = container_of(fabric, struct qdma_tcp_fabric, util_fabric.fabric_fid);
    if (!fab->xnet_fabric)
        return -FI_EINVAL;
    return fi_trywait(fab->xnet_fabric, fids, count);
}

struct qdma_tcp_pep
{
    struct fid_pep pep_fid;
    struct fid_pep *xnet_pep;
};

static int qdma_tcp_pep_close(struct fid *fid)
{
    struct qdma_tcp_pep *pep = container_of(fid, struct qdma_tcp_pep, pep_fid.fid);

    if (!pep)
        return -FI_EINVAL;

    if (pep->xnet_pep)
    {
        qdma_tcp_fid_map_remove(&pep->xnet_pep->fid);
        fi_close(&pep->xnet_pep->fid);
        pep->xnet_pep = NULL;
    }

    free(pep);
    return 0;
}

static int qdma_tcp_pep_bind(struct fid *fid, struct fid *bfid, uint64_t flags)
{
    struct qdma_tcp_pep *pep = container_of(fid, struct qdma_tcp_pep, pep_fid.fid);

    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;

    if (bfid && bfid->fclass == FI_CLASS_EQ && bfid->ops == &qdma_tcp_eq_fi_ops)
    {
        struct qdma_tcp_eq *qeq = container_of(bfid, struct qdma_tcp_eq, eq_fid.fid);
        int ret;

        if (!qeq || !qeq->xnet_eq)
            return -FI_EINVAL;
        ret = fi_pep_bind(pep->xnet_pep, &qeq->xnet_eq->fid, flags);
        if (!ret)
            qdma_tcp_eq_map_add(qeq, &pep->xnet_pep->fid, &pep->pep_fid.fid);
        return ret;
    }

    return fi_pep_bind(pep->xnet_pep, bfid, flags);
}

static int qdma_tcp_pep_control(struct fid *fid, int command, void *arg)
{
    struct qdma_tcp_pep *pep = container_of(fid, struct qdma_tcp_pep, pep_fid.fid);

    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;
    if (pep->xnet_pep->fid.ops && pep->xnet_pep->fid.ops->control)
        return pep->xnet_pep->fid.ops->control(&pep->xnet_pep->fid, command, arg);
    return fi_no_control(fid, command, arg);
}

static struct fi_ops qdma_tcp_pep_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_pep_close,
    .bind = qdma_tcp_pep_bind,
    .control = qdma_tcp_pep_control,
    .ops_open = fi_no_ops_open,
};

static struct fi_ops_ep qdma_tcp_pep_ep_ops = {
    .size = sizeof(struct fi_ops_ep),
    .cancel = fi_no_cancel,
    .getopt = fi_no_getopt,
    .setopt = fi_no_setopt,
    .tx_ctx = fi_no_tx_ctx,
    .rx_ctx = fi_no_rx_ctx,
    .rx_size_left = fi_no_rx_size_left,
    .tx_size_left = fi_no_tx_size_left,
};

static int qdma_tcp_pep_setname(fid_t fid, void *addr, size_t addrlen)
{
    struct qdma_tcp_pep *pep = container_of(fid, struct qdma_tcp_pep, pep_fid.fid);
    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;
    return fi_setname(&pep->xnet_pep->fid, addr, addrlen);
}

static int qdma_tcp_pep_getname(fid_t fid, void *addr, size_t *addrlen)
{
    struct qdma_tcp_pep *pep = container_of(fid, struct qdma_tcp_pep, pep_fid.fid);
    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;
    return fi_getname(&pep->xnet_pep->fid, addr, addrlen);
}

static int qdma_tcp_pep_listen(struct fid_pep *pep_fid)
{
    struct qdma_tcp_pep *pep = container_of(pep_fid, struct qdma_tcp_pep, pep_fid);
    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;
    return fi_listen(pep->xnet_pep);
}

static int qdma_tcp_pep_reject(struct fid_pep *pep_fid, fid_t handle,
                               const void *param, size_t paramlen)
{
    struct qdma_tcp_pep *pep = container_of(pep_fid, struct qdma_tcp_pep, pep_fid);
    if (!pep || !pep->xnet_pep)
        return -FI_EINVAL;
    return fi_reject(pep->xnet_pep, handle, param, paramlen);
}

static struct fi_ops_cm qdma_tcp_pep_cm_ops = {
    .size = sizeof(struct fi_ops_cm),
    .setname = qdma_tcp_pep_setname,
    .getname = qdma_tcp_pep_getname,
    .getpeer = fi_no_getpeer,
    .connect = fi_no_connect,
    .listen = qdma_tcp_pep_listen,
    .accept = fi_no_accept,
    .reject = qdma_tcp_pep_reject,
    .shutdown = fi_no_shutdown,
};

static int qdma_tcp_fabric_passive_ep(struct fid_fabric *fabric, struct fi_info *info,
                                      struct fid_pep **pep, void *context)
{
    struct qdma_tcp_fabric *fab = container_of(fabric, struct qdma_tcp_fabric, util_fabric.fabric_fid);
    struct fi_info *tmp_info;
    struct qdma_tcp_pep *qpep;
    int ret;

    if (!fab->xnet_fabric || !info || !pep)
        return -FI_EINVAL;

    tmp_info = fi_dupinfo(info);
    if (!tmp_info)
        return -FI_ENOMEM;

    if (tmp_info->fabric_attr)
        tmp_info->fabric_attr->prov_name = NULL;

    qpep = calloc(1, sizeof(*qpep));
    if (!qpep)
    {
        fi_freeinfo(tmp_info);
        return -FI_ENOMEM;
    }

    ret = fi_passive_ep(fab->xnet_fabric, tmp_info, &qpep->xnet_pep, context);
    fi_freeinfo(tmp_info);
    if (ret)
    {
        free(qpep);
        return ret;
    }

    qpep->pep_fid.fid.fclass = FI_CLASS_PEP;
    qpep->pep_fid.fid.context = context;
    qpep->pep_fid.fid.ops = &qdma_tcp_pep_fi_ops;
    qpep->pep_fid.ops = &qdma_tcp_pep_ep_ops;
    qpep->pep_fid.cm = &qdma_tcp_pep_cm_ops;

    qdma_tcp_fid_map_add(&qpep->xnet_pep->fid, &qpep->pep_fid.fid);

    *pep = &qpep->pep_fid;
    return 0;
}

static struct fi_ops_fabric qdma_tcp_fabric_ops = {
    .size = sizeof(struct fi_ops_fabric),
    .domain = qdma_tcp_fabric_domain,
    .passive_ep = qdma_tcp_fabric_passive_ep,
    .eq_open = qdma_tcp_fabric_eq_open,
    .wait_open = ofi_wait_fd_open,
    .trywait = qdma_tcp_fabric_trywait,
};

static int qdma_tcp_fabric(struct fi_fabric_attr *attr, struct fid_fabric **fabric, void *context)
{
    struct qdma_tcp_fabric *fab;
    int ret;
    if (!fabric)
        return -FI_EINVAL;
    fab = calloc(1, sizeof(*fab));
    if (!fab)
        return -FI_ENOMEM;
    ret = ofi_fabric_init(&qdma_tcp_provider, attr, attr, &fab->util_fabric, context);
    if (ret)
    {
        free(fab);
        return ret;
    }

    /* Backing xnet fabric for CM/EQ/PEP paths (used by fi_pingpong and rxm). */
    if (xnet_util_prov.prov && xnet_util_prov.prov->fabric)
    {
        struct fi_fabric_attr xnet_attr;
        memset(&xnet_attr, 0, sizeof(xnet_attr));
        if (attr)
            xnet_attr = *attr;
        xnet_attr.prov_name = NULL;

        ret = xnet_util_prov.prov->fabric(&xnet_attr, &fab->xnet_fabric, context);
        if (ret)
        {
            ofi_fabric_close(&fab->util_fabric);
            free(fab);
            return ret;
        }
    }

    fab->util_fabric.fabric_fid.fid.ops = &qdma_tcp_fabric_fi_ops;
    fab->util_fabric.fabric_fid.ops = &qdma_tcp_fabric_ops;
    *fabric = &fab->util_fabric.fabric_fid;
    return 0;
}

static void qdma_tcp_cleanup(void)
{
    /* Stop async worker if running */
#if defined(__linux__)
    qdma_async_fini();
#endif
}

/* ============================================================================
 * 4. DOMAIN OPERATIONS (fi_domain - CQ/AV/MR)
 * ============================================================================ */

struct qdma_tcp_cq_comp
{
    struct dlist_entry entry;
    void *op_context;
};

static void qdma_tcp_cq_enqueue(struct qdma_tcp_cq *cq, void *op_context)
{
    struct qdma_tcp_cq_comp *comp;

    if (!cq)
        return;

    comp = calloc(1, sizeof(*comp));
    if (!comp)
        return;
    comp->op_context = op_context;

    ofi_mutex_lock(&cq->lock);
    dlist_insert_tail(&comp->entry, &cq->comp_queue);
    ofi_mutex_unlock(&cq->lock);
}

#if defined(__linux__)
static void qdma_tcp_progress_poll_ep(struct qdma_tcp_ep *ep)
{
    if (!ep || ep->qdev_fd <= 0 || !ep->rx_cq)
        return;

    ofi_mutex_lock(&ep->recv_lock);

    struct qdma_recv_entry *recv_entry;
    struct dlist_entry *item, *tmp;
    dlist_foreach_safe(&ep->recv_queue, item, tmp)
    {
        recv_entry = container_of(item, struct qdma_recv_entry, entry);

        ssize_t bytes = read(ep->qdev_fd, recv_entry->buf, recv_entry->len);
        if (bytes > 0)
        {
            dlist_remove(item);
            ofi_mutex_unlock(&ep->recv_lock);

            FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
                   "QDMA RX: read %zd bytes into buffer %p\n", bytes, recv_entry->buf);

            qdma_tcp_cq_enqueue(ep->rx_cq, recv_entry->context ? recv_entry->context : ep);
            free(recv_entry);

            ofi_mutex_lock(&ep->recv_lock);
            continue;
        }

        if (bytes == 0)
        {
            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA, "Unexpected EOF on QDMA device\n");
            break;
        }

        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            break;

        FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA, "QDMA read error: %s\n", strerror(errno));
        break;
    }

    ofi_mutex_unlock(&ep->recv_lock);
}
#endif

static void qdma_tcp_cq_progress_eps(struct qdma_tcp_cq *cq)
{
    if (!cq)
        return;

#if defined(__linux__)
    /* Only RX-bound endpoints are tracked on this list (see ep_bind). */
    struct dlist_entry *item;
    dlist_foreach(&cq->bound_eps, item)
    {
        struct qdma_tcp_ep *ep = container_of(item, struct qdma_tcp_ep, rx_cq_entry);
        qdma_tcp_progress_poll_ep(ep);
    }
#endif
}

static ssize_t qdma_tcp_cq_read(struct fid_cq *cq_fid, void *buf, size_t count)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    size_t done = 0;

    if (!cq || !buf)
        return -FI_EINVAL;

    qdma_tcp_cq_progress_eps(cq);

    ofi_mutex_lock(&cq->lock);
    while (done < count && !dlist_empty(&cq->comp_queue))
    {
        struct dlist_entry *e = cq->comp_queue.next;
        struct qdma_tcp_cq_comp *comp = container_of(e, struct qdma_tcp_cq_comp, entry);
        dlist_remove(e);

        /* FI_CQ_FORMAT_CONTEXT: first field is op_context */
        ((struct fi_cq_entry *)buf)[done].op_context = comp->op_context;
        free(comp);
        done++;
    }
    ofi_mutex_unlock(&cq->lock);

    if (done)
        return (ssize_t)done;

    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->read)
        return cq->xnet_cq->ops->read(cq->xnet_cq, buf, count);

    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_cq_readfrom(struct fid_cq *cq_fid, void *buf, size_t count, fi_addr_t *src_addr)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    (void)src_addr;

    if (!cq)
        return -FI_EINVAL;

    /* QDMA internal completions do not carry src_addr. */
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->readfrom)
        return cq->xnet_cq->ops->readfrom(cq->xnet_cq, buf, count, src_addr);

    return qdma_tcp_cq_read(cq_fid, buf, count);
}

static ssize_t qdma_tcp_cq_readerr(struct fid_cq *cq_fid, struct fi_cq_err_entry *buf, uint64_t flags)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    if (!cq)
        return -FI_EINVAL;
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->readerr)
        return cq->xnet_cq->ops->readerr(cq->xnet_cq, buf, flags);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_cq_sread(struct fid_cq *cq_fid, void *buf, size_t count, const void *cond, int timeout)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    if (!cq)
        return -FI_EINVAL;
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->sread)
        return cq->xnet_cq->ops->sread(cq->xnet_cq, buf, count, cond, timeout);
    (void)cond;
    (void)timeout;
    return qdma_tcp_cq_read(cq_fid, buf, count);
}

static ssize_t qdma_tcp_cq_sreadfrom(struct fid_cq *cq_fid, void *buf, size_t count, fi_addr_t *src_addr,
                                     const void *cond, int timeout)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    if (!cq)
        return -FI_EINVAL;
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->sreadfrom)
        return cq->xnet_cq->ops->sreadfrom(cq->xnet_cq, buf, count, src_addr, cond, timeout);
    (void)cond;
    (void)timeout;
    return qdma_tcp_cq_readfrom(cq_fid, buf, count, src_addr);
}

static int qdma_tcp_cq_signal(struct fid_cq *cq_fid)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    if (!cq)
        return -FI_EINVAL;
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->signal)
        return cq->xnet_cq->ops->signal(cq->xnet_cq);
    return 0;
}

static const char *qdma_tcp_cq_strerror(struct fid_cq *cq_fid, int prov_errno,
                                        const void *err_data, char *buf, size_t len)
{
    struct qdma_tcp_cq *cq = container_of(cq_fid, struct qdma_tcp_cq, cq_fid);
    if (!cq)
        return "qdma_tcp";
    if (cq->xnet_cq && cq->xnet_cq->ops && cq->xnet_cq->ops->strerror)
        return cq->xnet_cq->ops->strerror(cq->xnet_cq, prov_errno, err_data, buf, len);
    return "qdma_tcp";
}

static int qdma_tcp_cq_close(struct fid *fid)
{
    struct qdma_tcp_cq *cq = container_of(fid, struct qdma_tcp_cq, cq_fid.fid);
    struct dlist_entry *item, *tmp;

    if (!cq)
        return -FI_EINVAL;

    if (cq->xnet_cq)
    {
        fi_close(&cq->xnet_cq->fid);
        cq->xnet_cq = NULL;
    }

    ofi_mutex_lock(&cq->lock);
    dlist_foreach_safe(&cq->comp_queue, item, tmp)
    {
        struct qdma_tcp_cq_comp *comp = container_of(item, struct qdma_tcp_cq_comp, entry);
        dlist_remove(item);
        free(comp);
    }
    ofi_mutex_unlock(&cq->lock);
    ofi_mutex_destroy(&cq->lock);

    free(cq);
    return 0;
}

static struct fi_ops qdma_tcp_cq_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_cq_close,
    .bind = fi_no_bind,
    .control = fi_no_control,
    .ops_open = fi_no_ops_open,
};

static struct fi_ops_cq qdma_tcp_cq_ops = {
    .size = sizeof(struct fi_ops_cq),
    .read = qdma_tcp_cq_read,
    .readfrom = qdma_tcp_cq_readfrom,
    .readerr = qdma_tcp_cq_readerr,
    .sread = qdma_tcp_cq_sread,
    .sreadfrom = qdma_tcp_cq_sreadfrom,
    .signal = qdma_tcp_cq_signal,
    .strerror = qdma_tcp_cq_strerror,
};

/* CQ operations */
static int qdma_tcp_cq_open(struct fid_domain *domain, struct fi_cq_attr *attr,
                            struct fid_cq **cq, void *context)
{
    struct qdma_tcp_domain *qd;
    struct qdma_tcp_cq *qcq;
    int ret;

    if (!domain || !cq || !attr)
        return -FI_EINVAL;

    qd = container_of(domain, struct qdma_tcp_domain, util_domain.domain_fid);
    if (!qd->xnet_domain)
        return -FI_EINVAL;

    qcq = calloc(1, sizeof(*qcq));
    if (!qcq)
        return -FI_ENOMEM;

    /* Create the underlying xnet CQ that the backing endpoint will use. */
    ret = fi_cq_open(qd->xnet_domain, attr, &qcq->xnet_cq, context);
    if (ret)
    {
        free(qcq);
        return ret;
    }

    qcq->format = attr->format;
    ofi_mutex_init(&qcq->lock);
    dlist_init(&qcq->comp_queue);
    dlist_init(&qcq->bound_eps);

    qcq->cq_fid.fid.fclass = FI_CLASS_CQ;
    qcq->cq_fid.fid.context = context;
    qcq->cq_fid.fid.ops = &qdma_tcp_cq_fi_ops;
    qcq->cq_fid.ops = &qdma_tcp_cq_ops;

    *cq = &qcq->cq_fid;
    return 0;
}

static int qdma_tcp_av_open(struct fid_domain *domain, struct fi_av_attr *attr,
                            struct fid_av **av, void *context)
{
    struct qdma_tcp_domain *qd;

    if (!domain || !av)
        return -FI_EINVAL;

    qd = container_of(domain, struct qdma_tcp_domain, util_domain.domain_fid);
    if (!qd->xnet_domain)
        return -FI_EINVAL;

    return fi_av_open(qd->xnet_domain, attr, av, context);
}

/* MR registration - forward to OFI util registration helpers for now */
static int qdma_tcp_mr_reg(struct fid *fid, const void *buf, size_t len,
                           uint64_t access, uint64_t offset, uint64_t requested_key,
                           uint64_t flags, struct fid_mr **mr_fid, void *context)
{
    return ofi_mr_reg(fid, buf, len, access, offset, requested_key, flags, mr_fid, context);
}

static int qdma_tcp_mr_regv(struct fid *fid, const struct iovec *iov,
                            size_t count, uint64_t access, uint64_t offset,
                            uint64_t requested_key, uint64_t flags,
                            struct fid_mr **mr_fid, void *context)
{
    return ofi_mr_regv(fid, iov, count, access, offset, requested_key, flags, mr_fid, context);
}

static int qdma_tcp_mr_regattr(struct fid *fid, const struct fi_mr_attr *attr,
                               uint64_t flags, struct fid_mr **mr_fid)
{
    return ofi_mr_regattr(fid, attr, flags, mr_fid);
}

static struct fi_ops_mr qdma_tcp_domain_fi_ops_mr = {
    .size = sizeof(struct fi_ops_mr),
    .reg = qdma_tcp_mr_reg,
    .regv = qdma_tcp_mr_regv,
    .regattr = qdma_tcp_mr_regattr,
};

static struct fi_ops_domain qdma_tcp_domain_ops = {
    .size = sizeof(struct fi_ops_domain),
    .av_open = qdma_tcp_av_open,
    .cq_open = qdma_tcp_cq_open,
    .endpoint = qdma_tcp_ep_open,
    .scalable_ep = fi_no_scalable_ep,
    .cntr_open = fi_no_cntr_open,
    .poll_open = fi_poll_create,
    .stx_ctx = fi_no_stx_context,
    .srx_ctx = fi_no_srx_context,
    .query_atomic = fi_no_query_atomic,
    .query_collective = fi_no_query_collective,
};
static int qdma_tcp_domain_open(struct fid_fabric *fabric_fid, struct fi_info *info,
                                struct fid_domain **domain_fid, void *context)
{
    struct qdma_tcp_domain *qd;
    struct qdma_tcp_fabric *fab;
    struct fi_info *tmp_info;
    int ret;

    if (!fabric_fid || !info || !domain_fid)
    {
        FI_WARN(&qdma_tcp_provider, FI_LOG_DOMAIN, "Invalid arguments to domain_open\n");
        return -FI_EINVAL;
    }

    qd = calloc(1, sizeof(*qd));
    if (!qd)
    {
        FI_WARN(&qdma_tcp_provider, FI_LOG_DOMAIN, "Failed to allocate domain\n");
        return -FI_ENOMEM;
    }

    ret = ofi_domain_init(fabric_fid, info, &qd->util_domain,
                          context, OFI_LOCK_NOOP);
    if (ret)
    {
        FI_WARN(&qdma_tcp_provider, FI_LOG_DOMAIN,
                "ofi_domain_init failed: %d\n", ret);
        free(qd);
        return ret;
    }

    /* Open backing xnet domain for compatibility (CM/CQ/AV/etc). */
    fab = container_of(fabric_fid, struct qdma_tcp_fabric, util_fabric.fabric_fid);
    if (!fab->xnet_fabric)
    {
        FI_WARN(&qdma_tcp_provider, FI_LOG_DOMAIN, "xnet backing fabric not initialized\n");
        ofi_domain_close(&qd->util_domain);
        free(qd);
        return -FI_EINVAL;
    }

    tmp_info = fi_dupinfo(info);
    if (!tmp_info)
    {
        ofi_domain_close(&qd->util_domain);
        free(qd);
        return -FI_ENOMEM;
    }
    if (tmp_info->fabric_attr)
        tmp_info->fabric_attr->prov_name = NULL;
    if (tmp_info->ep_attr)
        tmp_info->ep_attr->type = FI_EP_MSG;

    ret = fi_domain(fab->xnet_fabric, tmp_info, &qd->xnet_domain, context);
    fi_freeinfo(tmp_info);
    if (ret)
    {
        FI_WARN(&qdma_tcp_provider, FI_LOG_DOMAIN, "fi_domain (xnet) failed: %d\n", ret);
        ofi_domain_close(&qd->util_domain);
        free(qd);
        return ret;
    }

    qd->util_domain.domain_fid.fid.ops = &qdma_tcp_domain_fi_ops;
    qd->util_domain.domain_fid.ops = &qdma_tcp_domain_ops;
    qd->util_domain.domain_fid.mr = &qdma_tcp_domain_fi_ops_mr;

    FI_INFO(&qdma_tcp_provider, FI_LOG_DOMAIN,
            "Domain opened (queues managed via dma-ctl)\n");

    *domain_fid = &qd->util_domain.domain_fid;
    return 0;
}

static struct fi_ops qdma_tcp_domain_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_domain_close,
    .bind = ofi_domain_bind,
    .control = fi_no_control,
    .ops_open = fi_no_ops_open,
    .tostr = fi_no_tostr,
    .ops_set = fi_no_ops_set,
};

static int qdma_tcp_domain_close(struct fid *fid)
{
    if (!fid)
        return -FI_EINVAL;
    struct qdma_tcp_domain *qd = container_of(fid, struct qdma_tcp_domain, util_domain.domain_fid.fid);

    if (qd->xnet_domain)
    {
        fi_close(&qd->xnet_domain->fid);
        qd->xnet_domain = NULL;
    }

    /* No device cleanup needed - queues are managed externally via dma-ctl */

    /* cleanup util domain */
    ofi_domain_close(&qd->util_domain);

    free(qd);
    return 0;
}

/* ============================================================================
 * 5. ENDPOINT OPERATIONS (fi_endpoint - MSG/RMA/TAGGED)
 * ============================================================================ */

static ssize_t qdma_tcp_ep_cancel(fid_t fid, void *context);
static int qdma_tcp_ep_getopt(fid_t fid, int level, int optname, void *optval, size_t *optlen);
static int qdma_tcp_ep_setopt(fid_t fid, int level, int optname, const void *optval, size_t optlen);
static int qdma_tcp_ep_tx_ctx(struct fid_ep *sep, int index, struct fi_tx_attr *attr,
                              struct fid_ep **tx_ep, void *context);
static int qdma_tcp_ep_rx_ctx(struct fid_ep *sep, int index, struct fi_rx_attr *attr,
                              struct fid_ep **rx_ep, void *context);
static ssize_t qdma_tcp_ep_rx_size_left(struct fid_ep *ep);
static ssize_t qdma_tcp_ep_tx_size_left(struct fid_ep *ep);

static int qdma_tcp_cm_setname(fid_t fid, void *addr, size_t addrlen);
static int qdma_tcp_cm_getname(fid_t fid, void *addr, size_t *addrlen);
static int qdma_tcp_cm_getpeer(struct fid_ep *ep, void *addr, size_t *addrlen);
static int qdma_tcp_cm_connect(struct fid_ep *ep, const void *addr, const void *param, size_t paramlen);
static int qdma_tcp_cm_accept(struct fid_ep *ep, const void *param, size_t paramlen);
static int qdma_tcp_cm_shutdown(struct fid_ep *ep, uint64_t flags);

/* Endpoint close */
static int qdma_tcp_ep_close(struct fid *fid)
{
    struct qdma_tcp_ep *ep;

    if (!fid)
        return -FI_EINVAL;

    ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);

    /* No queue cleanup needed - queues are managed externally via dma-ctl */

#if defined(__linux__)
    /* Drain and cancel any queued async requests for this endpoint, then
     * wait for in-flight requests to finish before freeing the endpoint.
     */
    pthread_mutex_lock(&qdma_async.lock);
    ep->closing = 1;

    /* Remove queued requests for this ep and collect them locally so we can
     * post error completions and free them without holding the async lock.
     */
    struct qdma_io_req *cur = qdma_async.head;
    struct qdma_io_req *prev = NULL;
    struct qdma_io_req *removed_head = NULL;
    struct qdma_io_req *removed_tail = NULL;

    while (cur)
    {
        if (cur->ep == ep)
        {
            struct qdma_io_req *n = cur->next;
            /* unlink */
            if (prev)
                prev->next = n;
            else
                qdma_async.head = n;
            if (qdma_async.tail == cur)
                qdma_async.tail = prev;

            /* adjust counters */
            if (qdma_async.inflight > 0)
                qdma_async.inflight--;
            if (ep->async_inflight > 0)
                ep->async_inflight--;

            /* add to removed list */
            cur->next = NULL;
            if (removed_tail)
                removed_tail->next = cur;
            else
                removed_head = cur;
            removed_tail = cur;

            cur = n;
            continue;
        }
        prev = cur;
        cur = cur->next;
    }

    pthread_mutex_unlock(&qdma_async.lock);

    /* Post error completions for removed requests and free them */
    cur = removed_head;
    while (cur)
    {
        if (ep->tx_cq)
            qdma_tcp_cq_enqueue(ep->tx_cq, cur->ctx ? cur->ctx : ep);
        if (cur->iov)
            free(cur->iov);
        struct qdma_io_req *n = cur->next;
        free(cur);
        cur = n;
    }

    /* Wait for any in-flight operations for this ep to complete */
    pthread_mutex_lock(&qdma_async.lock);
    while (ep->async_inflight > 0)
        pthread_cond_wait(&qdma_async.cond, &qdma_async.lock);
    pthread_mutex_unlock(&qdma_async.lock);

    /* Close any opened QDMA char device fd */
    if (ep->qdev_fd > 0)
    {
        close(ep->qdev_fd);
        ep->qdev_fd = 0;
        ep->qdev_name[0] = '\0';
    }

    /* Clean up any remaining posted receives */
    ofi_mutex_lock(&ep->recv_lock);
    struct qdma_recv_entry *recv_entry;
    struct dlist_entry *item, *tmp;
    dlist_foreach_safe(&ep->recv_queue, item, tmp)
    {
        recv_entry = container_of(item, struct qdma_recv_entry, entry);
        dlist_remove(item);
        free(recv_entry);
    }
    ofi_mutex_unlock(&ep->recv_lock);
    ofi_mutex_destroy(&ep->recv_lock);
#endif

    if (ep->xnet_ep)
    {
        qdma_tcp_fid_map_remove(&ep->xnet_ep->fid);
        fi_close(&ep->xnet_ep->fid);
        ep->xnet_ep = NULL;
    }

    if (ep->rx_cq && ep->rx_cq_entry.next != &ep->rx_cq_entry)
    {
        dlist_remove(&ep->rx_cq_entry);
        dlist_init(&ep->rx_cq_entry);
    }

    free(ep);
    return 0;
}

static int qdma_tcp_ep_bind(struct fid *fid, struct fid *fid_to_bind, uint64_t flags)
{
    struct qdma_tcp_ep *ep;
    struct fid *target = fid_to_bind;

    if (!fid)
        return -FI_EINVAL;

    ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (!ep->xnet_ep)
        return -FI_EINVAL;

    if (fid_to_bind && fid_to_bind->fclass == FI_CLASS_CQ &&
        fid_to_bind->ops == &qdma_tcp_cq_fi_ops)
    {
        struct qdma_tcp_cq *qcq = container_of(fid_to_bind, struct qdma_tcp_cq, cq_fid.fid);
        if (!qcq->xnet_cq)
            return -FI_EINVAL;
        target = &qcq->xnet_cq->fid;

        if (flags & FI_TRANSMIT)
            ep->tx_cq = qcq;
        if (flags & FI_RECV)
        {
            ep->rx_cq = qcq;
            if (ep->rx_cq_entry.next == &ep->rx_cq_entry)
                dlist_insert_tail(&ep->rx_cq_entry, &qcq->bound_eps);
        }
    }

    if (fid_to_bind && fid_to_bind->fclass == FI_CLASS_EQ &&
        fid_to_bind->ops == &qdma_tcp_eq_fi_ops)
    {
        struct qdma_tcp_eq *qeq = container_of(fid_to_bind, struct qdma_tcp_eq, eq_fid.fid);

        if (!qeq->xnet_eq)
            return -FI_EINVAL;
        target = &qeq->xnet_eq->fid;
        /* Ensure CM events refer to the wrapper endpoint, not xnet ep. */
        qdma_tcp_eq_map_add(qeq, &ep->xnet_ep->fid, &ep->ep_fid.fid);
    }

    return fi_ep_bind(ep->xnet_ep, target, flags);
}

static int qdma_tcp_ep_control(struct fid *fid, int command, void *arg)
{
    struct qdma_tcp_ep *ep;

    if (!fid)
        return -FI_EINVAL;

    ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep->xnet_ep && ep->xnet_ep->fid.ops && ep->xnet_ep->fid.ops->control)
        return ep->xnet_ep->fid.ops->control(&ep->xnet_ep->fid, command, arg);

    return fi_no_control(fid, command, arg);
}

static struct fi_ops qdma_tcp_ep_fi_ops = {
    .size = sizeof(struct fi_ops),
    .close = qdma_tcp_ep_close,
    .bind = qdma_tcp_ep_bind,
    .control = qdma_tcp_ep_control,
    .ops_open = fi_no_ops_open,
};

static struct fi_ops_ep qdma_tcp_ep_ops = {
    .size = sizeof(struct fi_ops_ep),
    .cancel = qdma_tcp_ep_cancel,
    .getopt = qdma_tcp_ep_getopt,
    .setopt = qdma_tcp_ep_setopt,
    .tx_ctx = qdma_tcp_ep_tx_ctx,
    .rx_ctx = qdma_tcp_ep_rx_ctx,
    .rx_size_left = qdma_tcp_ep_rx_size_left,
    .tx_size_left = qdma_tcp_ep_tx_size_left,
};

static struct fi_ops_cm qdma_tcp_cm_ops = {
    .size = sizeof(struct fi_ops_cm),
    .setname = qdma_tcp_cm_setname,
    .getname = qdma_tcp_cm_getname,
    .getpeer = qdma_tcp_cm_getpeer,
    .connect = qdma_tcp_cm_connect,
    .listen = fi_no_listen,
    .accept = qdma_tcp_cm_accept,
    .reject = fi_no_reject,
    .shutdown = qdma_tcp_cm_shutdown,
    .join = fi_no_join,
};

static ssize_t qdma_tcp_ep_cancel(fid_t fid, void *context)
{
    struct qdma_tcp_ep *ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->cancel)
        return ep->xnet_ep_ops->cancel((fid_t)ep->xnet_ep, context);
    return -FI_ENOSYS;
}

static int qdma_tcp_ep_getopt(fid_t fid, int level, int optname, void *optval, size_t *optlen)
{
    struct qdma_tcp_ep *ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->getopt)
        return ep->xnet_ep_ops->getopt((fid_t)ep->xnet_ep, level, optname, optval, optlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_ep_setopt(fid_t fid, int level, int optname, const void *optval, size_t optlen)
{
    struct qdma_tcp_ep *ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->setopt)
        return ep->xnet_ep_ops->setopt((fid_t)ep->xnet_ep, level, optname, optval, optlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_ep_tx_ctx(struct fid_ep *sep, int index, struct fi_tx_attr *attr,
                              struct fid_ep **tx_ep, void *context)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->tx_ctx)
        return ep->xnet_ep_ops->tx_ctx(ep->xnet_ep, index, attr, tx_ep, context);
    return -FI_ENOSYS;
}

static int qdma_tcp_ep_rx_ctx(struct fid_ep *sep, int index, struct fi_rx_attr *attr,
                              struct fid_ep **rx_ep, void *context)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->rx_ctx)
        return ep->xnet_ep_ops->rx_ctx(ep->xnet_ep, index, attr, rx_ep, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_ep_rx_size_left(struct fid_ep *sep)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->rx_size_left)
        return ep->xnet_ep_ops->rx_size_left(ep->xnet_ep);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_ep_tx_size_left(struct fid_ep *sep)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_ep_ops && ep->xnet_ep_ops->tx_size_left)
        return ep->xnet_ep_ops->tx_size_left(ep->xnet_ep);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_setname(fid_t fid, void *addr, size_t addrlen)
{
    struct qdma_tcp_ep *ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->setname)
        return ep->xnet_cm_ops->setname((fid_t)ep->xnet_ep, addr, addrlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_getname(fid_t fid, void *addr, size_t *addrlen)
{
    struct qdma_tcp_ep *ep = container_of(fid, struct qdma_tcp_ep, ep_fid.fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->getname)
        return ep->xnet_cm_ops->getname((fid_t)ep->xnet_ep, addr, addrlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_getpeer(struct fid_ep *sep, void *addr, size_t *addrlen)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->getpeer)
        return ep->xnet_cm_ops->getpeer(ep->xnet_ep, addr, addrlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_connect(struct fid_ep *sep, const void *addr, const void *param, size_t paramlen)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->connect)
        return ep->xnet_cm_ops->connect(ep->xnet_ep, addr, param, paramlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_accept(struct fid_ep *sep, const void *param, size_t paramlen)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->accept)
        return ep->xnet_cm_ops->accept(ep->xnet_ep, param, paramlen);
    return -FI_ENOSYS;
}

static int qdma_tcp_cm_shutdown(struct fid_ep *sep, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(sep, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_cm_ops && ep->xnet_cm_ops->shutdown)
        return ep->xnet_cm_ops->shutdown(ep->xnet_ep, flags);
    return -FI_ENOSYS;
}

/* Message operations */
static ssize_t qdma_tcp_msg_recv(struct fid_ep *ep_fid, void *buf, size_t len,
                                 void *desc, fi_addr_t src_addr, void *context);
static ssize_t qdma_tcp_msg_send(struct fid_ep *ep_fid, const void *buf, size_t len,
                                 void *desc, fi_addr_t dest_addr, void *context);
static ssize_t qdma_tcp_msg_sendv(struct fid_ep *ep_fid, const struct iovec *iov,
                                  void **desc, size_t count, fi_addr_t dest_addr, void *context);
static ssize_t qdma_tcp_msg_sendmsg(struct fid_ep *ep_fid, const struct fi_msg *msg, uint64_t flags);
static ssize_t qdma_tcp_msg_recvv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                  size_t count, fi_addr_t src_addr, void *context);
static ssize_t qdma_tcp_msg_recvmsg(struct fid_ep *ep_fid, const struct fi_msg *msg, uint64_t flags);

static ssize_t qdma_tcp_msg_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                   fi_addr_t dest_addr);

static ssize_t qdma_tcp_rma_read(struct fid_ep *ep_fid, void *buf, size_t len, void *desc,
                                 fi_addr_t src_addr, uint64_t addr, uint64_t key, void *context);
static ssize_t qdma_tcp_rma_readv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                  size_t count, fi_addr_t src_addr, uint64_t addr, uint64_t key,
                                  void *context);
static ssize_t qdma_tcp_rma_readmsg(struct fid_ep *ep_fid, const struct fi_msg_rma *msg, uint64_t flags);
static ssize_t qdma_tcp_rma_write(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                  fi_addr_t dest_addr, uint64_t addr, uint64_t key, void *context);
static ssize_t qdma_tcp_rma_writev(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                   size_t count, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                                   void *context);
static ssize_t qdma_tcp_rma_writemsg(struct fid_ep *ep_fid, const struct fi_msg_rma *msg, uint64_t flags);
static ssize_t qdma_tcp_rma_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                   fi_addr_t dest_addr, uint64_t addr, uint64_t key);
static ssize_t qdma_tcp_rma_writedata(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                      uint64_t data, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                                      void *context);
static ssize_t qdma_tcp_rma_injectdata(struct fid_ep *ep_fid, const void *buf, size_t len,
                                       uint64_t data, fi_addr_t dest_addr, uint64_t addr, uint64_t key);

static ssize_t qdma_tcp_tagged_recv(struct fid_ep *ep_fid, void *buf, size_t len, void *desc,
                                    fi_addr_t src_addr, uint64_t tag, uint64_t ignore, void *context);
static ssize_t qdma_tcp_tagged_recvv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                     size_t count, fi_addr_t src_addr, uint64_t tag, uint64_t ignore,
                                     void *context);
static ssize_t qdma_tcp_tagged_recvmsg(struct fid_ep *ep_fid, const struct fi_msg_tagged *msg, uint64_t flags);
static ssize_t qdma_tcp_tagged_send(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                    fi_addr_t dest_addr, uint64_t tag, void *context);
static ssize_t qdma_tcp_tagged_sendv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                     size_t count, fi_addr_t dest_addr, uint64_t tag, void *context);
static ssize_t qdma_tcp_tagged_sendmsg(struct fid_ep *ep_fid, const struct fi_msg_tagged *msg, uint64_t flags);
static ssize_t qdma_tcp_tagged_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                      fi_addr_t dest_addr, uint64_t tag);
static ssize_t qdma_tcp_tagged_senddata(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                        uint64_t data, fi_addr_t dest_addr, uint64_t tag, void *context);
static ssize_t qdma_tcp_tagged_injectdata(struct fid_ep *ep_fid, const void *buf, size_t len,
                                          uint64_t data, fi_addr_t dest_addr, uint64_t tag);

/* Endpoint ops tables */
static struct fi_ops_msg qdma_tcp_msg_ops = {
    .size = sizeof(struct fi_ops_msg),
    .send = qdma_tcp_msg_send,
    .recv = qdma_tcp_msg_recv,
    .inject = qdma_tcp_msg_inject,
    .sendmsg = qdma_tcp_msg_sendmsg,
    .recvmsg = qdma_tcp_msg_recvmsg,
    .sendv = qdma_tcp_msg_sendv,
    .recvv = qdma_tcp_msg_recvv,
};

static struct fi_ops_rma qdma_tcp_rma_ops = {
    .size = sizeof(struct fi_ops_rma),
    .read = qdma_tcp_rma_read,
    .readv = qdma_tcp_rma_readv,
    .readmsg = qdma_tcp_rma_readmsg,
    .write = qdma_tcp_rma_write,
    .writev = qdma_tcp_rma_writev,
    .writemsg = qdma_tcp_rma_writemsg,
    .inject = qdma_tcp_rma_inject,
    .writedata = qdma_tcp_rma_writedata,
    .injectdata = qdma_tcp_rma_injectdata,
};

static struct fi_ops_tagged qdma_tcp_tagged_ops = {
    .size = sizeof(struct fi_ops_tagged),
    .recv = qdma_tcp_tagged_recv,
    .recvv = qdma_tcp_tagged_recvv,
    .recvmsg = qdma_tcp_tagged_recvmsg,
    .send = qdma_tcp_tagged_send,
    .sendv = qdma_tcp_tagged_sendv,
    .sendmsg = qdma_tcp_tagged_sendmsg,
    .inject = qdma_tcp_tagged_inject,
    .senddata = qdma_tcp_tagged_senddata,
    .injectdata = qdma_tcp_tagged_injectdata,
};

static ssize_t qdma_tcp_msg_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                   fi_addr_t dest_addr)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->inject)
        return ep->xnet_msg_ops->inject(ep->xnet_ep, buf, len, dest_addr);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_read(struct fid_ep *ep_fid, void *buf, size_t len, void *desc,
                                 fi_addr_t src_addr, uint64_t addr, uint64_t key, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->read)
        return ep->xnet_rma_ops->read(ep->xnet_ep, buf, len, desc, src_addr, addr, key, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_readv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                  size_t count, fi_addr_t src_addr, uint64_t addr, uint64_t key,
                                  void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->readv)
        return ep->xnet_rma_ops->readv(ep->xnet_ep, iov, desc, count, src_addr, addr, key, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_readmsg(struct fid_ep *ep_fid, const struct fi_msg_rma *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->readmsg)
        return ep->xnet_rma_ops->readmsg(ep->xnet_ep, msg, flags);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_write(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                  fi_addr_t dest_addr, uint64_t addr, uint64_t key, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->write)
        return ep->xnet_rma_ops->write(ep->xnet_ep, buf, len, desc, dest_addr, addr, key, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_writev(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                   size_t count, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                                   void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->writev)
        return ep->xnet_rma_ops->writev(ep->xnet_ep, iov, desc, count, dest_addr, addr, key, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_writemsg(struct fid_ep *ep_fid, const struct fi_msg_rma *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->writemsg)
        return ep->xnet_rma_ops->writemsg(ep->xnet_ep, msg, flags);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                   fi_addr_t dest_addr, uint64_t addr, uint64_t key)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->inject)
        return ep->xnet_rma_ops->inject(ep->xnet_ep, buf, len, dest_addr, addr, key);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_writedata(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                      uint64_t data, fi_addr_t dest_addr, uint64_t addr, uint64_t key,
                                      void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->writedata)
        return ep->xnet_rma_ops->writedata(ep->xnet_ep, buf, len, desc, data, dest_addr, addr, key, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_rma_injectdata(struct fid_ep *ep_fid, const void *buf, size_t len,
                                       uint64_t data, fi_addr_t dest_addr, uint64_t addr, uint64_t key)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_rma_ops && ep->xnet_rma_ops->injectdata)
        return ep->xnet_rma_ops->injectdata(ep->xnet_ep, buf, len, data, dest_addr, addr, key);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_recv(struct fid_ep *ep_fid, void *buf, size_t len, void *desc,
                                    fi_addr_t src_addr, uint64_t tag, uint64_t ignore, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->recv)
        return ep->xnet_tagged_ops->recv(ep->xnet_ep, buf, len, desc, src_addr, tag, ignore, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_recvv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                     size_t count, fi_addr_t src_addr, uint64_t tag, uint64_t ignore,
                                     void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->recvv)
        return ep->xnet_tagged_ops->recvv(ep->xnet_ep, iov, desc, count, src_addr, tag, ignore, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_recvmsg(struct fid_ep *ep_fid, const struct fi_msg_tagged *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->recvmsg)
        return ep->xnet_tagged_ops->recvmsg(ep->xnet_ep, msg, flags);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_send(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                    fi_addr_t dest_addr, uint64_t tag, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->send)
        return ep->xnet_tagged_ops->send(ep->xnet_ep, buf, len, desc, dest_addr, tag, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_sendv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                     size_t count, fi_addr_t dest_addr, uint64_t tag, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->sendv)
        return ep->xnet_tagged_ops->sendv(ep->xnet_ep, iov, desc, count, dest_addr, tag, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_sendmsg(struct fid_ep *ep_fid, const struct fi_msg_tagged *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->sendmsg)
        return ep->xnet_tagged_ops->sendmsg(ep->xnet_ep, msg, flags);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_inject(struct fid_ep *ep_fid, const void *buf, size_t len,
                                      fi_addr_t dest_addr, uint64_t tag)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->inject)
        return ep->xnet_tagged_ops->inject(ep->xnet_ep, buf, len, dest_addr, tag);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_senddata(struct fid_ep *ep_fid, const void *buf, size_t len, void *desc,
                                        uint64_t data, fi_addr_t dest_addr, uint64_t tag, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->senddata)
        return ep->xnet_tagged_ops->senddata(ep->xnet_ep, buf, len, desc, data, dest_addr, tag, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_tagged_injectdata(struct fid_ep *ep_fid, const void *buf, size_t len,
                                          uint64_t data, fi_addr_t dest_addr, uint64_t tag)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_tagged_ops && ep->xnet_tagged_ops->injectdata)
        return ep->xnet_tagged_ops->injectdata(ep->xnet_ep, buf, len, data, dest_addr, tag);
    return -FI_ENOSYS;
}

/* Message receive wrappers matching fi_ops_msg signature.
 * For QDMA fast-path: post buffer to recv_queue for later read().
 * For TCP fallback: delegate to util.
 */
static ssize_t qdma_tcp_msg_recv(struct fid_ep *ep_fid, void *buf, size_t len,
                                 void *desc, fi_addr_t src_addr, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    (void)desc;
    (void)src_addr;

#if defined(__linux__)
    /* If QDMA device available, post receive buffer for async read */
    if (ep && ep->qdev_fd > 0)
    {
        struct qdma_recv_entry *recv_entry = calloc(1, sizeof(*recv_entry));
        if (!recv_entry)
        {
            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA,
                    "Failed to allocate recv entry\n");
            return -FI_ENOMEM;
        }

        recv_entry->buf = buf;
        recv_entry->len = len;
        recv_entry->context = context;

        ofi_mutex_lock(&ep->recv_lock);
        dlist_insert_tail(&recv_entry->entry, &ep->recv_queue);
        ofi_mutex_unlock(&ep->recv_lock);

        FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
               "Posted recv buffer %p len=%zu\n", buf, len);
        return 0;
    }
#endif

    /* Fall back to util TCP */
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->recv)
        return ep->xnet_msg_ops->recv(ep->xnet_ep, buf, len, desc, src_addr, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_msg_recvv(struct fid_ep *ep_fid, const struct iovec *iov, void **desc,
                                  size_t count, fi_addr_t src_addr, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->recvv)
        return ep->xnet_msg_ops->recvv(ep->xnet_ep, iov, desc, count, src_addr, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_msg_recvmsg(struct fid_ep *ep_fid, const struct fi_msg *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);

#if defined(__linux__)
    if (ep && ep->qdev_fd > 0)
    {
        if (msg && msg->msg_iov && msg->iov_count > 0)
            return qdma_tcp_msg_recvv(ep_fid, msg->msg_iov, msg->desc, msg->iov_count,
                                      msg->addr, msg->context);
        if (msg && msg->msg_iov && msg->iov_count == 1)
            return qdma_tcp_msg_recv(ep_fid, msg->msg_iov[0].iov_base, msg->msg_iov[0].iov_len,
                                     NULL, msg->addr, msg->context);
    }
#endif

    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->recvmsg)
        return ep->xnet_msg_ops->recvmsg(ep->xnet_ep, msg, flags);

    return -FI_ENOSYS;
}

/* Message send helpers: try QDMA fast-path then fall back to util (TCP) */

/* Context-aware contiguous send helper. Uses user ctx for completions. */
static ssize_t qdma_tcp_msg_send_contig_ctx(struct fid_ep *ep_fid, const void *buf,
                                            size_t len, void *desc, fi_addr_t dest_addr, void *ctx)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);

    (void)desc;
    (void)dest_addr;

#if defined(__linux__)
    /* Try QDMA fast-path if char device is available */
    if (ep && ep->qdev_fd > 0)
    {
        int write_fd = ep->qdev_fd;
        const char *p = (const char *)buf;
        size_t left = len;
        size_t written = 0;

        while (left > 0)
        {
            ssize_t w = write(write_fd, p, left);
            if (w > 0)
            {
                written += (size_t)w;
                left -= (size_t)w;
                p += w;
                continue;
            }
            if (w == 0)
            {
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA,
                        "Unexpected zero return from write()\n");
                break;
            }
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
                       "Write would block, submitting to async (written=%zu, remaining=%zu)\n",
                       written, left);
                struct iovec iov = {(void *)p, left};
                int sret = qdma_async_submit_tx(ep, &iov, 1, left, ctx);
                if (sret == 0)
                    return 0; /* accepted; completion will be posted asynchronously */
                return sret;  /* propagate error */
            }
            /* other error -> log and fall back */
            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA,
                    "QDMA write error after %zu bytes: %s\n", written, strerror(errno));
            break;
        }
        if (left == 0)
        {
            if (ep->tx_cq)
                qdma_tcp_cq_enqueue(ep->tx_cq, ctx ? ctx : ep);
            FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
                   "QDMA TX: sent %zu bytes\n", len);
            return 0;
        }
        /* Fall through to TCP on partial write or error */
        FI_INFO(&qdma_tcp_provider, FI_LOG_EP_DATA,
                "Falling back to TCP after QDMA partial/error\n");
    }
#endif

    /* Fall back to util/TCP if QDMA path not available or failed */
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->send)
        return ep->xnet_msg_ops->send(ep->xnet_ep, buf, len, desc, dest_addr, ctx);

    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_msg_send(struct fid_ep *ep_fid, const void *buf, size_t len,
                                 void *desc, fi_addr_t dest_addr, void *context)
{
    return qdma_tcp_msg_send_contig_ctx(ep_fid, buf, len, desc, dest_addr, context);
}

static ssize_t qdma_tcp_msg_sendv(struct fid_ep *ep_fid, const struct iovec *iov,
                                  void **desc, size_t count, fi_addr_t dest_addr, void *context)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);
    (void)dest_addr;

    if (!iov || count == 0)
        return -FI_EINVAL;

    size_t total = 0;
    for (size_t i = 0; i < count; ++i)
        total += iov[i].iov_len;

    if (total == 0)
        return 0;

#if defined(__linux__)
    /* Zero-copy path: use writev() directly if QDMA device available */
    if (ep && ep->qdev_fd > 0)
    {
        ssize_t written = 0;
        size_t remaining = total;
        const struct iovec *cur_iov = iov;
        int cur_count = count;

        while (remaining > 0 && cur_count > 0)
        {
            ssize_t w = writev(ep->qdev_fd, cur_iov, cur_count);

            if (w > 0)
            {
                written += w;
                remaining -= (size_t)w;

                /* Advance past completed iovecs */
                size_t skip = (size_t)w;
                while (skip > 0 && cur_count > 0)
                {
                    if (cur_iov->iov_len <= skip)
                    {
                        skip -= cur_iov->iov_len;
                        cur_iov++;
                        cur_count--;
                    }
                    else
                    {
                        /* Partial iovec write - would need to adjust base/len */
                        break;
                    }
                }

                if (skip > 0)
                {
                    /* Partial write in middle of iovec - submit remainder to async */
                    FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
                           "Partial writev, submitting remainder to async\n");
                    int sret = qdma_async_submit_tx(ep, (struct iovec *)cur_iov, cur_count, remaining, context);
                    if (sret == 0)
                        return 0; /* accepted for async completion */
                    return sret;  /* error from async queueing */
                }
                continue;
            }

            if (w == 0)
                break;

            if (errno == EINTR)
                continue;

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                /* Submit remainder to async worker */
                int sret = qdma_async_submit_tx(ep, (struct iovec *)cur_iov, cur_count, remaining, context);
                if (sret == 0)
                    return 0; /* accepted; completion to follow asynchronously */
                return sret;  /* propagate async enqueue error */
            }

            /* Other error */
            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_DATA,
                    "writev error: %s\n", strerror(errno));
            break;
        }

        if (remaining == 0)
        {
            /* Complete success */
            if (ep->tx_cq)
                qdma_tcp_cq_enqueue(ep->tx_cq, context ? context : ep);
            FI_DBG(&qdma_tcp_provider, FI_LOG_EP_DATA,
                   "QDMA TX: sent %zu bytes via writev\n", total);
            return 0;
        }
    }
#endif

    /* TCP fallback: delegate to util provider */
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->sendv)
        return ep->xnet_msg_ops->sendv(ep->xnet_ep, iov, desc, count, dest_addr, context);
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->send)
        return ep->xnet_msg_ops->send(ep->xnet_ep, iov[0].iov_base, iov[0].iov_len,
                                      desc ? desc[0] : NULL, dest_addr, context);
    return -FI_ENOSYS;
}

static ssize_t qdma_tcp_msg_sendmsg(struct fid_ep *ep_fid, const struct fi_msg *msg, uint64_t flags)
{
    struct qdma_tcp_ep *ep = container_of(ep_fid, struct qdma_tcp_ep, ep_fid);

    if (!msg)
        return -FI_EINVAL;

#if defined(__linux__)
    /* Prefer to collapse to a contiguous buffer so we can attach msg->context
     * to any async QDMA submissions.
     */
    if (ep && ep->qdev_fd > 0 && msg->msg_iov && msg->iov_count > 0)
    {
        size_t total = 0;
        for (size_t i = 0; i < msg->iov_count; ++i)
            total += msg->msg_iov[i].iov_len;
        if (total == 0)
            return 0;
        char *tmp = malloc(total);
        if (!tmp)
            return -FI_ENOMEM;
        char *p = tmp;
        for (size_t i = 0; i < msg->iov_count; ++i)
        {
            memcpy(p, msg->msg_iov[i].iov_base, msg->msg_iov[i].iov_len);
            p += msg->msg_iov[i].iov_len;
        }
        ssize_t ret = qdma_tcp_msg_send_contig_ctx(ep_fid, tmp, total, NULL, msg->addr, msg->context);
        free(tmp);
        return ret;
    }
#endif

    /* TCP fallback: delegate to util provider */
    if (ep && ep->xnet_msg_ops && ep->xnet_msg_ops->sendmsg)
        return ep->xnet_msg_ops->sendmsg(ep->xnet_ep, msg, flags);
    return -FI_ENOSYS;
}

static int qdma_tcp_ep_open(struct fid_domain *domain, struct fi_info *info,
                            struct fid_ep **ep_fid, void *context)
{
    struct qdma_tcp_ep *ep;
    struct qdma_tcp_domain *qd;
    struct fi_info *tmp_info;
    int ret;

    if (!domain || !ep_fid)
        return -FI_EINVAL;

    ep = calloc(1, sizeof(*ep));
    if (!ep)
        return -FI_ENOMEM;

    qd = container_of(domain, struct qdma_tcp_domain, util_domain.domain_fid);
    ep->domain = qd;

    dlist_init(&ep->tx_cq_entry);
    dlist_init(&ep->rx_cq_entry);

    /* Initialize receive queue for posted receives */
    dlist_init(&ep->recv_queue);
    ofi_mutex_init(&ep->recv_lock);

    if (!qd->xnet_domain)
    {
        ofi_mutex_destroy(&ep->recv_lock);
        free(ep);
        return -FI_EINVAL;
    }

    tmp_info = fi_dupinfo(info);
    if (!tmp_info)
    {
        ofi_mutex_destroy(&ep->recv_lock);
        free(ep);
        return -FI_ENOMEM;
    }
    if (tmp_info->fabric_attr)
        tmp_info->fabric_attr->prov_name = NULL;

    /* When stacked under ofi_rxm, the incoming fi_info may request RDM/DGRAM.
     * The backing xnet provider only supports FI_EP_MSG, so force that for the
     * internal endpoint we create.
     */
    if (tmp_info->ep_attr)
        tmp_info->ep_attr->type = FI_EP_MSG;

    ret = fi_endpoint(qd->xnet_domain, tmp_info, &ep->xnet_ep, context);
    fi_freeinfo(tmp_info);
    if (ret)
    {
        ofi_mutex_destroy(&ep->recv_lock);
        free(ep);
        return ret;
    }

    ep->xnet_msg_ops = ep->xnet_ep->msg;
    ep->xnet_rma_ops = ep->xnet_ep->rma;
    ep->xnet_tagged_ops = ep->xnet_ep->tagged;
    ep->xnet_cm_ops = ep->xnet_ep->cm;
    ep->xnet_ep_ops = ep->xnet_ep->ops;

    memset(&ep->ep_fid, 0, sizeof(ep->ep_fid));
    ep->ep_fid.fid.fclass = FI_CLASS_EP;
    ep->ep_fid.fid.context = context;
    ep->ep_fid.fid.ops = &qdma_tcp_ep_fi_ops;
    ep->ep_fid.ops = &qdma_tcp_ep_ops;
    ep->ep_fid.cm = &qdma_tcp_cm_ops;
    ep->ep_fid.msg = &qdma_tcp_msg_ops;
    ep->ep_fid.rma = &qdma_tcp_rma_ops;
    ep->ep_fid.tagged = &qdma_tcp_tagged_ops;

    /* Allow EQ wrapper to translate xnet EP fids -> wrapper EP fid. */
    qdma_tcp_fid_map_add(&ep->xnet_ep->fid, &ep->ep_fid.fid);

#if defined(__linux__)
    /* Open pre-configured QDMA queue character device if specified.
     * Queue must be created and started externally via dma-ctl before running app.
     */
    const char *qdev = getenv("QDMA_QUEUE_DEV");

    if (qdev != NULL)
    {
        int fd = open(qdev, O_RDWR | O_NONBLOCK);
        if (fd >= 0)
        {
            ep->qdev_fd = fd;
            strncpy(ep->qdev_name, qdev, sizeof(ep->qdev_name) - 1);
            FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "Opened QDMA queue device %s (fd=%d) for fast-path\n",
                    ep->qdev_name, ep->qdev_fd);
        }
        else
        {
            /* Provide helpful error message with setup instructions */
            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "Failed to open QDMA queue device %s: %s\n",
                    qdev, strerror(errno));

            if (errno == ENOENT)
            {
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "Device not found. To setup QDMA queues:\n");
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "  1. Check QDMA driver: lsmod | grep qdma\n");
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "  2. Run: sudo ./setup_qdma_queues.sh <device> <num_queues> st\n");
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "  3. Verify: ls -la /dev/qdma*\n");
            }
            else if (errno == EACCES || errno == EPERM)
            {
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "Permission denied. Check device permissions:\n");
                FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                        "  sudo chmod 666 %s\n", qdev);
            }

            FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "Falling back to TCP transport\n");
            ep->qdev_fd = -1;
        }
    }
    else
    {
        /* Auto-detect: try to find any available QDMA queue device */
        int found = 0;
        char auto_dev[256];

        /* Try common patterns: /dev/qdma*-ST-0, /dev/qdma*-MM-0 */
        const char *patterns[] = {
            "/dev/qdma*-ST-0",
            "/dev/qdma*-MM-0",
            NULL};

        for (int i = 0; patterns[i] != NULL && !found; i++)
        {
            /* Simple glob: just try a few common device names */
            for (int dev_num = 1000; dev_num <= 1015 && !found; dev_num++)
            {
                snprintf(auto_dev, sizeof(auto_dev), "/dev/qdma0%d000-ST-0", dev_num);
                if (access(auto_dev, F_OK) == 0)
                {
                    FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                            "Auto-detected QDMA device: %s\n", auto_dev);
                    FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                            "Consider setting: export QDMA_QUEUE_DEV=%s\n", auto_dev);
                    qdev = auto_dev;
                    found = 1;

                    int fd = open(qdev, O_RDWR | O_NONBLOCK);
                    if (fd >= 0)
                    {
                        ep->qdev_fd = fd;
                        strncpy(ep->qdev_name, qdev, sizeof(ep->qdev_name) - 1);
                        FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                                "Using auto-detected device for fast-path\n");
                    }
                    else
                    {
                        FI_WARN(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                                "Found %s but cannot open: %s\n", auto_dev, strerror(errno));
                        ep->qdev_fd = -1;
                    }
                    break;
                }
            }
        }

        if (!found)
        {
            FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "QDMA_QUEUE_DEV not set and no devices auto-detected - using TCP only\n");
            FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "To enable QDMA fast-path:\n");
            FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "  1. Setup queues: sudo ./setup_qdma_queues.sh <device> 1 st\n");
            FI_INFO(&qdma_tcp_provider, FI_LOG_EP_CTRL,
                    "  2. Set env var: export QDMA_QUEUE_DEV=/dev/<device>-ST-0\n");
            ep->qdev_fd = -1;
        }
    }
#endif

    *ep_fid = &ep->ep_fid;
    return 0;
}
