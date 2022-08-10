#ifndef COMMON_H
#define COMMON_H

#include <arrow/api.h>

#define PARQUET_FILE_DIR "./flight_datasets/"
#define PARQUET_FILE_NAME "trade.parquet"
#define PARQUET_ROWGROUP_RECORDS 10000
#define RECORD_ROW_NUM 10000000
#define SERVER_PORT 33000

std::shared_ptr<arrow::Schema> getSchema()
{
    return arrow::schema({arrow::field("sno", arrow::utf8()),
                          arrow::field("trdno", arrow::utf8()),
                          arrow::field("trddate", arrow::date32()),
                          arrow::field("loref", arrow::utf8()),
                          arrow::field("bsf", arrow::boolean()),
                          arrow::field("oso", arrow::utf8()),
                          arrow::field("comid", arrow::utf8()),
                          arrow::field("trderid", arrow::utf8()),
                          arrow::field("fid", arrow::utf8()),
                          arrow::field("cuid", arrow::utf8()),
                          arrow::field("olf", arrow::boolean()),
                          arrow::field("pri", arrow::float64()),
                          arrow::field("qty", arrow::int64()),
                          arrow::field("osn", arrow::utf8()),
                          arrow::field("op", arrow::float64()),
                          arrow::field("oppfi", arrow::utf8()),
                          arrow::field("oppcuid", arrow::utf8()),
                          arrow::field("opptrdrid", arrow::utf8()),
                          arrow::field("tw", arrow::utf8()),
                          arrow::field("hf", arrow::utf8()),
                          arrow::field("tf", arrow::utf8()),
                          arrow::field("fof", arrow::utf8()),
                          arrow::field("cmty", arrow::utf8()),
                          arrow::field("orty", arrow::utf8()),
                          arrow::field("otd", arrow::date32()),
                          arrow::field("tv", arrow::float64()),
                          arrow::field("tc", arrow::float64()),
                          arrow::field("lp", arrow::float64()),
                          arrow::field("prem", arrow::float64()),
                          arrow::field("lcp", arrow::float64())});
}

#endif