
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/dataset/dataset.h>
#include <arrow/compute/api.h>
#include <arrow/compute/registry.h>

#include <iostream>
using namespace std;

#include "common.h"

namespace cp = arrow::compute;

arrow::Status opers()
{
    ARROW_ASSIGN_OR_RAISE(auto arr1, CreateArray());
    ARROW_ASSIGN_OR_RAISE(auto arr2, CreateArray());

    {
        // 加
        ARROW_ASSIGN_OR_RAISE(arrow::Datum tmp_datum,
                              arrow::compute::CallFunction("add", {arr1, arr2}));
        const std::shared_ptr<arrow::Array> output_array = tmp_datum.make_array();
        cout << output_array->ToString() << endl;
    }
    {
        // 减
        ARROW_ASSIGN_OR_RAISE(arrow::Datum tmp_datum,
                              arrow::compute::CallFunction("subtract", {arr1, arr2}));
        const std::shared_ptr<arrow::Array> output_array = tmp_datum.make_array();
        cout << output_array->ToString() << endl;
    }
    {
        // 乘
        ARROW_ASSIGN_OR_RAISE(arrow::Datum tmp_datum,
                              arrow::compute::CallFunction("multiply", {arr1, arr2}));
        const std::shared_ptr<arrow::Array> output_array = tmp_datum.make_array();
        cout << output_array->ToString() << endl;
    }
    {
        // 除
        ARROW_ASSIGN_OR_RAISE(arrow::Datum tmp_datum,
                              arrow::compute::CallFunction("divide", {arr1, arr2}));
        const std::shared_ptr<arrow::Array> output_array = tmp_datum.make_array();
        cout << output_array->ToString() << endl;
    }
    {
        // 最大最小值
        // 设置参数
        ARROW_ASSIGN_OR_RAISE(auto arr3, CreateArrayWithNull());
        cout << arr3->ToString() << endl;
        arrow::compute::ScalarAggregateOptions scalar_aggregate_options;
        scalar_aggregate_options.skip_nulls = false;
        ARROW_ASSIGN_OR_RAISE(arrow::Datum min_max, arrow::compute::CallFunction("min_max", {arr3}, &scalar_aggregate_options));
        cout << min_max.scalar_as<arrow::StructScalar>().ToString() << endl;
    }

    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    showAllComputeFuncNames();
    cout << opers() << endl;
    return 0;
}
