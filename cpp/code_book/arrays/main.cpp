
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <vector>
#include <set>
using namespace std;

arrow::Status assignInsert()
{
    arrow::Int32Builder builder;
    ARROW_RETURN_NOT_OK(builder.Append(1));
    ARROW_RETURN_NOT_OK(builder.Append(2));
    ARROW_RETURN_NOT_OK(builder.Append(3));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, builder.Finish())
    cout << arr->ToString() << std::endl;
    return arrow::Status::OK();
}

arrow::Status stl2Arrow()
{
    // Raw pointers
    arrow::Int64Builder long_builder = arrow::Int64Builder();
    std::array<int64_t, 4> values = {1, 2, 3, 4};
    ARROW_RETURN_NOT_OK(long_builder.AppendValues(values.data(), values.size()));
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, long_builder.Finish()); // 使用Builder将数据从STL容器放入Array中
    cout << arr->ToString() << std::endl;

    // Vectors
    arrow::StringBuilder str_builder = arrow::StringBuilder();
    std::vector<std::string> strvals = {"x", "y", "z"};
    ARROW_RETURN_NOT_OK(str_builder.AppendValues(strvals));
    ARROW_ASSIGN_OR_RAISE(arr, str_builder.Finish());
    cout << arr->ToString() << std::endl;

    // Iterators
    arrow::DoubleBuilder dbl_builder = arrow::DoubleBuilder();
    std::set<double> dblvals = {1.1, 1.1, 2.3};
    ARROW_RETURN_NOT_OK(dbl_builder.AppendValues(dblvals.begin(), dblvals.end()));
    ARROW_ASSIGN_OR_RAISE(arr, dbl_builder.Finish());
    cout << arr->ToString() << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    assignInsert();
    stl2Arrow();
    return 0;
}
