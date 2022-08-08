
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <iostream>
#include <vector>
#include <set>
#include <random>
using namespace std;

class RandomBatchGenerator
{

public:
    std::shared_ptr<arrow::Schema> schema;
    RandomBatchGenerator(std::shared_ptr<arrow::Schema> schema) : schema(schema){};
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Generate(int32_t num_rows)
    {
        num_rows_ = num_rows;
        for (std::shared_ptr<arrow::Field> field : schema->fields())
        {
            ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field->type(), this));
        }
        return arrow::RecordBatch::Make(schema, num_rows, arrays_);
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType &type)
    {
        cout << "visit invalid type:" << type.ToString() << endl;
        return arrow::Status::NotImplemented("Generating data for", type.ToString());
    }

    arrow::Status Visit(const arrow::DoubleType &)
    {
        auto builder = arrow::DoubleBuilder();
        std::normal_distribution<> d{/*mean=*/5.0, /*stddev=*/2.0}; // 正态分布
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            builder.Append(d(gen_));
        }

        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListType &type)
    {
        // Generate offsets first, which determines number of values in sub-array
        std::poisson_distribution<> d{/*mean=*/4}; // 产生随机非负整数值i，按离散概率函数分布
        auto builder = arrow::Int32Builder();
        builder.Append(0); // 因为ARROW_ASSIGN_OR_RAISE要求必须至少有一个元素，否则会core dump

        int32_t last_val = 0;
        for (int32_t i = 0; i < num_rows_; ++i)
        {
            last_val += d(gen_);
            builder.Append(last_val);
        }
        ARROW_ASSIGN_OR_RAISE(auto offsets, builder.Finish());
        cout << "!!" << offsets->ToString() << endl;
        // 子列表长度自定，所以需要一个新的生成器。类型设定为List中的元素类型
        RandomBatchGenerator value_gen(arrow::schema({arrow::field("x", type.value_type())}));
        // 设置offsets列表的所有元素的值之和为子列表的长度
        ARROW_ASSIGN_OR_RAISE(auto inner_batch, value_gen.Generate(last_val));

        // offsets保存0-随机数，则在array中添加(随机数+1)个数量的值，FromArrays用法见testFromArrays
        std::shared_ptr<arrow::Array> values = inner_batch->column(0);
        ARROW_ASSIGN_OR_RAISE(auto array, arrow::ListArray::FromArrays(*offsets.get(), *values.get()));
        arrays_.push_back(array);
        return arrow::Status::OK();
    }

protected:
    std::random_device rd_{};
    std::mt19937 gen_{rd_()}; // 随机种子
    std::vector<std::shared_ptr<arrow::Array>> arrays_;
    int32_t num_rows_;

}; // RandomBatchGenerator

arrow::Status func()
{
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("x", arrow::float64()),
                       arrow::field("y", arrow::list(arrow::float64()))});

    RandomBatchGenerator generator(schema);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, generator.Generate(1));

    cout << "Created batch: " << endl
         << batch->ToString();

    // Consider using ValidateFull to check correctness
    ARROW_RETURN_NOT_OK(batch->ValidateFull());

    return arrow::Status::OK();
}

arrow::Status testFromArrays()
{
    arrow::Int32Builder int32_builder;
    int32_builder.Append(0);
    int32_builder.Append(2);
    int32_builder.Append(5);
    std::shared_ptr<arrow::Array> offsets;
    // TODO 学习如何遍历？

    ARROW_ASSIGN_OR_RAISE(offsets, int32_builder.Finish());

    arrow::FloatBuilder float_builder;
    float_builder.Append(8.0);
    float_builder.Append(7.0);
    float_builder.Append(6.0);
    float_builder.Append(5.0);
    float_builder.Append(4.0);
    float_builder.Append(3.0);
    
    std::shared_ptr<arrow::Array> values;
    ARROW_ASSIGN_OR_RAISE(values, float_builder.Finish());

    ARROW_ASSIGN_OR_RAISE(auto array, arrow::ListArray::FromArrays(*offsets.get(), *values.get()));
    cout << array->ToString() << endl;

    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    cout << "----" << endl;
    testFromArrays();
    return 0;
}
