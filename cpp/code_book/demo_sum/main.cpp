
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <vector>
using namespace std;

class TableSummation
{
    double partial = 0.0;

public:
    arrow::Result<double> Compute(std::shared_ptr<arrow::RecordBatch> batch)
    {
        for (std::shared_ptr<arrow::Array> array : batch->columns())
        {
            ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array, this));
        }
        return partial;
    }

    // Default implementation

    arrow::Status Visit(const arrow::Array &array)
    {
        return arrow::Status::NotImplemented("Can not compute sum for array of type ", array.type()->ToString());
    }

    template <typename ArrayType, typename T = typename ArrayType::TypeClass>
    arrow::enable_if_number<T, arrow::Status> Visit(const ArrayType &array)
    {
        for (arrow::util::optional<typename T::c_type> value : array)
        {
            if (value.has_value())
            {
                partial += static_cast<double>(value.value());
            }
        }
        return arrow::Status::OK();
    }

}; // TableSummation

arrow::Status func()
{
    std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::float64()),
    });
    int32_t num_rows = 3;
    std::vector<std::shared_ptr<arrow::Array>> columns;

    arrow::Int32Builder a_builder = arrow::Int32Builder();
    std::vector<int32_t> a_vals = {1, 2, 3};
    ARROW_RETURN_NOT_OK(a_builder.AppendValues(a_vals));
    ARROW_ASSIGN_OR_RAISE(auto a_arr, a_builder.Finish());
    columns.push_back(a_arr);

    arrow::DoubleBuilder b_builder = arrow::DoubleBuilder();
    std::vector<double> b_vals = {4.0, 5.0, 6.0};
    ARROW_RETURN_NOT_OK(b_builder.AppendValues(b_vals));
    ARROW_ASSIGN_OR_RAISE(auto b_arr, b_builder.Finish());
    columns.push_back(b_arr);

    auto batch = arrow::RecordBatch::Make(schema, num_rows, columns);

    // Call
    TableSummation summation;
    ARROW_ASSIGN_OR_RAISE(auto total, summation.Compute(batch));

    cout << "Total is " << total;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    func();
    return 0;
}
