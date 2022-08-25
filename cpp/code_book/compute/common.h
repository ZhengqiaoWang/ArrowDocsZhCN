#ifndef COMMON_H
#define COMMON_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>

/**
 * @brief 生成一系列用于展示的数据
 *
 * @return 数据表
 */
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable()
{
    auto schema =
        arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64()),
                       arrow::field("c", arrow::int64())});
    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
    arrow::NumericBuilder<arrow::Int64Type> builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));
    return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

void showAllComputeFuncNames()
{
    auto registry = arrow::compute::GetFunctionRegistry();
    for (auto &name : registry->GetFunctionNames())
    {
        cout << name << ", ";
    }
}
#endif