dnl Configury specific to the libfabric qdma_tcp provider

AC_DEFUN([FI_QDMA_TCP_CONFIGURE],[
	qdma_tcp_happy=0

	AS_IF([test x"$enable_qdma_tcp" != x"no"], [
		AS_IF([test "$linux" -eq 1], [
			AC_CHECK_HEADER([sys/uio.h], [qdma_tcp_happy=1], [qdma_tcp_happy=0])
		], [
			qdma_tcp_happy=0
		])
	])

	dnl Hard requirement: tcp provider must be enabled AND built-in,
	dnl because qdma_tcp currently references internal tcp symbols.
	AS_IF([test $qdma_tcp_happy -eq 1], [
		AS_IF([test "${tcp_happy:-0}" -ne 1], [qdma_tcp_happy=0])
		AS_IF([test "${tcp_dl:-0}" -ne 0], [qdma_tcp_happy=0])
	])
])
