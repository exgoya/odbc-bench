// odbc_c_bench_dsnless.c (fixed)
// Build: gcc -O2 -pthread -o odbc_c_bench_dsnless odbc_c_bench_dsnless.c -lodbc
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sql.h>
#include <sqlext.h>

typedef struct {
    const char* driver_path;
    const char* driver_name;
    const char* server;
    const char* port;
    const char* user;
    const char* pwd;
    const char* database;
    const char* extra;
    const char* query;
    long arraysize;      // 1000~10000 권장
    int  nthr;           // threads
    long max_rows;       // 0=unlimited
    int  readonly;       // 1=READONLY
    int  verbose;
} Opt;

typedef struct {
    long   rows;
    double elapsed_s;
    double fetch_mean_ms;
    double fetch_p95_ms;
    int    error;
} Stat;

typedef struct {
    Opt*  opt;
    Stat* stats;
    int   idx;
} Arg;

static double now_sec(void){
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void diag(SQLSMALLINT htype, SQLHANDLE h, const char* where){
    SQLSMALLINT i=1; SQLINTEGER nat; SQLCHAR state[7], text[1024]; SQLSMALLINT len;
    fprintf(stderr,"[ODBC][%s]\n", where);
    while (SQLGetDiagRec(htype, h, i, state, &nat, text, sizeof(text), &len) == SQL_SUCCESS){
        fprintf(stderr,"  %s (%ld): %s\n", state, (long)nat, text);
        i++;
    }
}

#define MAX_COLS 64
#define COL_BUFSZ 256

typedef struct {
    SQLSMALLINT ncols;
    SQLULEN     rows_fetched;              // ODBC: ROWS_FETCHED_PTR is SQLULEN*
    SQLCHAR*    colbufs[MAX_COLS];
    SQLLEN*     colinds[MAX_COLS];         // indicator array per column (size arraysize)
} BindCtx;

static int bind_rowset(SQLHSTMT st, BindCtx* b, SQLULEN arraysize){
    memset(b,0,sizeof(*b));
    if (SQLNumResultCols(st, &b->ncols) != SQL_SUCCESS) return -1;
    if (b->ncols > MAX_COLS){ fprintf(stderr,"Too many cols (%d)\n", b->ncols); return -1; }

    if (SQLSetStmtAttr(st, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)SQL_BIND_BY_COLUMN, 0) != SQL_SUCCESS) return -1;
    if (SQLSetStmtAttr(st, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)(SQLULEN)arraysize, 0) != SQL_SUCCESS) return -1;
    if (SQLSetStmtAttr(st, SQL_ATTR_ROWS_FETCHED_PTR, (SQLPOINTER)&b->rows_fetched, 0) != SQL_SUCCESS) return -1;

    for (int c=0;c<b->ncols;c++){
        b->colbufs[c] = (SQLCHAR*)malloc((size_t)COL_BUFSZ * (size_t)arraysize);
        b->colinds[c] = (SQLLEN*)  malloc(sizeof(SQLLEN) * (size_t)arraysize);
        if (!b->colbufs[c] || !b->colinds[c]) return -1;

        if (SQLBindCol(st, (SQLUSMALLINT)(c+1), SQL_C_CHAR,
                       b->colbufs[c], COL_BUFSZ, b->colinds[c]) != SQL_SUCCESS) return -1;
    }
    return 0;
}
static void free_rowset(BindCtx* b){
    for (int c=0;c<b->ncols;c++){ free(b->colbufs[c]); free(b->colinds[c]); }
}

static void build_connstr(const Opt* o, char* out, size_t outsz){
    const char* drv = NULL;
    if (o->driver_path && o->driver_path[0])      drv = o->driver_path;      // absolute .so path
    else if (o->driver_name && o->driver_name[0]) drv = o->driver_name;      // requires odbcinst
    else drv = "";

    // NOTE: READONLY=1 은 드라이버별 지원 여부가 다를 수 있음.
    // 확실히 하려면 연결 후 SQLSetConnectAttr(SQL_ATTR_ACCESS_MODE, SQL_MODE_READ_ONLY).
    snprintf(out, outsz,
        "Driver=%s;Server=%s;Port=%s;UID=%s;PWD=%s;Database=%s;AUTOCOMMIT=1;%s%s",
        drv,
        o->server   ? o->server   : "",
        o->port     ? o->port     : "3306",
        o->user     ? o->user     : "",
        o->pwd      ? o->pwd      : "",
        o->database ? o->database : "",
        o->readonly ? "READONLY=1;" : "",
        o->extra    ? o->extra    : ""
    );
}

static void* worker(void* ap){
    Arg* a=(Arg*)ap; Opt* o=a->opt; Stat* s=&a->stats[a->idx];
    s->rows=0; s->elapsed_s=0; s->fetch_mean_ms=0; s->fetch_p95_ms=0; s->error=0;

    // (best-effort) Disable DM pooling before any ENV
    SQLSetEnvAttr(NULL, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_OFF, 0);

    SQLHENV env=SQL_NULL_HENV; SQLHDBC dbc=SQL_NULL_HDBC; SQLHSTMT st=SQL_NULL_HSTMT;
    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env) != SQL_SUCCESS){ s->error=1; goto done; }
    if (SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0) != SQL_SUCCESS){ s->error=1; goto done; }
    if (SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) != SQL_SUCCESS){ s->error=1; goto done; }

    char connstr[2048]; build_connstr(o, connstr, sizeof(connstr));
    SQLCHAR out[1024]; SQLSMALLINT outlen=0;
    if (SQLDriverConnect(dbc, NULL, (SQLCHAR*)connstr, SQL_NTS, out, sizeof(out), &outlen, SQL_DRIVER_NOPROMPT) != SQL_SUCCESS){
        diag(SQL_HANDLE_DBC, dbc, "connect"); s->error=1; goto done;
    }

    // 확실한 Read-Only (드라이버가 지원하면 성공)
    if (o->readonly) SQLSetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);

    if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &st) != SQL_SUCCESS){ s->error=1; goto done; }
    SQLSetStmtAttr(st, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, 0);
    SQLSetStmtAttr(st, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_READ_ONLY, 0);

    double t0 = now_sec();
    if (SQLExecDirect(st, (SQLCHAR*)o->query, SQL_NTS) != SQL_SUCCESS){
        diag(SQL_HANDLE_STMT, st, "exec"); s->error=1; goto done;
    }

    BindCtx bc;
    if (bind_rowset(st, &bc, (SQLULEN)o->arraysize) != 0){ diag(SQL_HANDLE_STMT, st, "bind"); s->error=1; goto done; }

    const int MAXS=10000; double *samples=(double*)malloc(sizeof(double)*MAXS); int ns=0;
    while (1){
        double fb=now_sec();
        SQLRETURN rc = SQLFetchScroll(st, SQL_FETCH_NEXT, 0);
        double fe=now_sec();

        if (rc==SQL_NO_DATA) break;
        if (!SQL_SUCCEEDED(rc)){ diag(SQL_HANDLE_STMT, st, "fetch"); s->error=1; break; }
        s->rows += bc.rows_fetched;
        if (o->max_rows>0 && s->rows >= o->max_rows) break;
        if (ns<MAXS) samples[ns++] = (fe-fb)*1000.0;
    }
    double t1 = now_sec();
    s->elapsed_s = t1 - t0;

    if (ns>0){
        double sum=0; for(int i=0;i<ns;i++) sum+=samples[i];
        s->fetch_mean_ms = sum/(double)ns;
        int k=(int)(0.95*ns); if (k>=ns) k=ns-1;
        // partial selection for p95
        for(int i=0;i<=k;i++){ int m=i; for(int j=i+1;j<ns;j++) if(samples[j]<samples[m]) m=j; double tmp=samples[i]; samples[i]=samples[m]; samples[m]=tmp; }
        s->fetch_p95_ms = samples[k];
    }
    free(samples);
    free_rowset(&bc);

done:
    if (st)  SQLFreeHandle(SQL_HANDLE_STMT, st);
    if (dbc){ SQLDisconnect(dbc); SQLFreeHandle(SQL_HANDLE_DBC, dbc); }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
    return NULL;
}

static void usage(const char* p){
    fprintf(stderr,
      "Usage: %s --query SQL --threads N --arraysize M \n"
      "  [--driver-path /path/to/libmsqlodbcw.so | --driver-name \"Driver Name\"]\n"
      "  --server HOST [--port 3306] --user U --pwd P --database DB\n"
      "  [--extra \"SSL=0;CHARSET=UTF8;\"] [--max-rows K] [--readonly] [--verbose]\n", p);
}

int main(int argc, char** argv){
    Opt o={0}; o.arraysize=1000; o.nthr=1; o.port="3306";
    for (int i=1;i<argc;i++){
        if (!strcmp(argv[i],"--driver-path") && i+1<argc) o.driver_path=argv[++i];
        else if (!strcmp(argv[i],"--driver-name") && i+1<argc) o.driver_name=argv[++i];
        else if (!strcmp(argv[i],"--server") && i+1<argc) o.server=argv[++i];
        else if (!strcmp(argv[i],"--port") && i+1<argc) o.port=argv[++i];
        else if (!strcmp(argv[i],"--user") && i+1<argc) o.user=argv[++i];
        else if (!strcmp(argv[i],"--pwd") && i+1<argc) o.pwd=argv[++i];
        else if (!strcmp(argv[i],"--database") && i+1<argc) o.database=argv[++i];
        else if (!strcmp(argv[i],"--extra") && i+1<argc) o.extra=argv[++i];
        else if (!strcmp(argv[i],"--query") && i+1<argc) o.query=argv[++i];
        else if (!strcmp(argv[i],"--threads") && i+1<argc) o.nthr=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--arraysize") && i+1<argc) o.arraysize=atol(argv[++i]);
        else if (!strcmp(argv[i],"--max-rows") && i+1<argc) o.max_rows=atol(argv[++i]);
        else if (!strcmp(argv[i],"--readonly")) o.readonly=1;
        else if (!strcmp(argv[i],"--verbose")) o.verbose=1;
        else { usage(argv[0]); return 1; }
    }
    if ((!o.driver_path && !o.driver_name) || !o.server || !o.user || !o.pwd || !o.database || !o.query){ usage(argv[0]); return 1; }

    pthread_t* tids=(pthread_t*)calloc(o.nthr,sizeof(pthread_t));
    Stat*      stats=(Stat*)calloc(o.nthr,sizeof(Stat));
    Arg*       args=(Arg*)calloc(o.nthr,sizeof(Arg));

    double w0=now_sec();
    for (int t=0;t<o.nthr;t++){ args[t].opt=&o; args[t].stats=stats; args[t].idx=t;
        if (pthread_create(&tids[t], NULL, worker, &args[t])){ perror("pthread_create"); return 2; } }
    for (int t=0;t<o.nthr;t++) pthread_join(tids[t], NULL);
    double w1=now_sec();

    long total_rows=0; int err=0; double sum_mean=0, max_p95=0;
    for (int t=0;t<o.nthr;t++){
        total_rows += stats[t].rows; err |= stats[t].error;
        sum_mean += stats[t].fetch_mean_ms; if (stats[t].fetch_p95_ms > max_p95) max_p95 = stats[t].fetch_p95_ms;
        if (o.verbose){
            double rps = (stats[t].rows>0 && stats[t].elapsed_s>0) ? (stats[t].rows / stats[t].elapsed_s) : 0.0;
            printf("thr %d: rows=%ld elapsed=%.3fs rps=%.0f mean=%.2fms p95=%.2fms%s\n",
                t, stats[t].rows, stats[t].elapsed_s, rps, stats[t].fetch_mean_ms, stats[t].fetch_p95_ms, stats[t].error?" [ERR]":"");
        }
    }
    double wall = w1 - w0;
    double rps_all = (total_rows>0 && wall>0) ? (double)total_rows / wall : 0.0;
    printf("threads=%d arraysize=%ld → rows=%ld wall=%.3fs rows/sec=%.0f mean(fetch)=%.2fms p95(fetch)=%.2fms%s\n",
           o.nthr, o.arraysize, total_rows, wall, rps_all,
           (o.nthr? sum_mean/o.nthr:0.0), max_p95, err?" [ERROR]":"");

    free(tids); free(stats); free(args);
    return err?3:0;
}

