# config/ldap.m4

# PGAC_LDAP_SAFE
# --------------
# PostgreSQL sometimes loads libldap_r and plain libldap into the same
# process.  Check for OpenLDAP versions known not to tolerate doing so; assume
# non-OpenLDAP implementations are safe.  The dblink test suite exercises the
# hazardous interaction directly.

AC_DEFUN([PGAC_LDAP_SAFE],
[AC_CACHE_CHECK([for compatible LDAP implementation], [pgac_cv_ldap_safe],
[AC_COMPILE_IFELSE([AC_LANG_PROGRAM(
[#include <ldap.h>
#if !defined(LDAP_VENDOR_VERSION) || \
     (defined(LDAP_API_FEATURE_X_OPENLDAP) && \
      LDAP_VENDOR_VERSION >= 20424 && LDAP_VENDOR_VERSION <= 20431)
choke me
#endif], [])],
[pgac_cv_ldap_safe=yes],
[pgac_cv_ldap_safe=no])])

if test "$pgac_cv_ldap_safe" != yes; then
  AC_MSG_WARN([
*** With OpenLDAP versions 2.4.24 through 2.4.31, inclusive, each backend
*** process that loads libpq (via WAL receiver, dblink, or postgres_fdw) and
*** also uses LDAP will crash on exit.])
fi])



