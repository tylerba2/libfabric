#ifndef QDMA_TCP_INTERNAL_H
#define QDMA_TCP_INTERNAL_H

/* Use the canonical libfabric headers */
#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_trigger.h>
#include <rdma/providers/fi_prov.h>

/* util helpers types (util_ep, util_cq etc) */
#include <ofi_util.h>

/* Provider metadata */
#define QDMA_TCP_PROVIDER_NAME "qdma_tcp"
#define QDMA_TCP_FI_VERSION FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION)

/* Provider-internal structures */
struct qdma_tcp_fabric
{
    struct util_fabric util_fabric;
    struct fid_fabric *xnet_fabric;
};

struct qdma_tcp_domain
{
    struct util_domain util_domain; /* must be the exported fid */
    struct fid_domain *xnet_domain;
    /* No device handle - queues are managed externally via dma-ctl */
};

/* CQ wrapper: delegates to an underlying xnet CQ but can also surface
 * provider-generated completions (e.g., QDMA fast-path completions).
 */
struct qdma_tcp_cq
{
    struct fid_cq cq_fid;
    struct fid_cq *xnet_cq;
    enum fi_cq_format format;

    ofi_mutex_t lock;
    struct dlist_entry comp_queue;

    struct dlist_entry bound_eps;
};

/* Endpoint state for qdma_tcp provider */
struct qdma_tcp_ep
{
    struct fid_ep ep_fid;           /* exported endpoint fid */
    struct qdma_tcp_domain *domain; /* back pointer to domain */

    struct fid_ep *xnet_ep; /* backing xnet endpoint */
    const struct fi_ops_msg *xnet_msg_ops;
    const struct fi_ops_rma *xnet_rma_ops;
    const struct fi_ops_tagged *xnet_tagged_ops;

    struct fi_ops_cm *xnet_cm_ops;
    struct fi_ops_ep *xnet_ep_ops;

    struct qdma_tcp_cq *tx_cq;
    struct qdma_tcp_cq *rx_cq;

    struct dlist_entry tx_cq_entry;
    struct dlist_entry rx_cq_entry;

    /* QDMA character device fd and name (for fast-path data I/O) */
    int qdev_fd;        /* fd for /dev/qdmaXXXXX-ST-Y, or -1 if TCP-only */
    char qdev_name[64]; /* device path for logging */

    /* Async operation tracking */
    int async_inflight; /* number of async requests in flight for this ep */
    int closing;        /* set when ep is closing to stop new work */

    /* Posted receive buffers for QDMA RX (C2H) */
    struct dlist_entry recv_queue; /* list of posted receive buffers */
    ofi_mutex_t recv_lock;         /* protect recv_queue */
};

/* Posted receive buffer entry */
struct qdma_recv_entry
{
    struct dlist_entry entry;
    void *buf;
    size_t len;
    void *context; /* user's completion context */
};

#endif /* QDMA_TCP_INTERNAL_H */
