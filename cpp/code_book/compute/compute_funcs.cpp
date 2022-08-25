
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

arrow::Status func()
{
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());
    ARROW_ASSIGN_OR_RAISE(arrow::Datum incremented_datum,
                          arrow::compute::CallFunction("add", {table->column(0), table->column(1)}));
    std::shared_ptr<arrow::Array> incremented_array = std::move(incremented_datum).make_array();
    cout << incremented_array->ToString() << endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    // showAllComputeFuncNames();
    cout << func() << endl;
    return 0;
}
