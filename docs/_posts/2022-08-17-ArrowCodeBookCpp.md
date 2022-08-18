---
layout: article
title: Apache Arrow C++ Codebook
sidebar:
   nav: navigator
aside:
   toc: true
---

*目录
{:toc}

本编文档收集了展示了一些使用者可能在使用Arrow开发时面临的通用场景。文档中的示例为如何解决此类事物提供了强力且有效的解决方案。

## 使用C++接口

本章会介绍一些只要你要用C++接口就必须要清楚的概念。

### 使用Status和Result

C++库一般不得不选择使用抛出异常或者返回错误码来提示错误。Arrow选择返回Status和Result，这就比使用整数作返回值更容易清楚失败的原因和时机。

每次检查操作的Status是否成功是十分重要的，不过这个就显得比较乏味了：

```c++
std::function<arrow::Status()> test_fn = [] {
  arrow::NullBuilder builder;
  arrow::Status st = builder.Reserve(2);
  // Tedious return value check
  if (!st.ok()) {
    return st;
  }
  st = builder.AppendNulls(-1);
  // Tedious return value check
  if (!st.ok()) {
    return st;
  }
  rout << "Appended -1 null values?" << std::endl;
  return arrow::Status::OK();
};
arrow::Status st = test_fn();
rout << st << std::endl;
```

output:

```text
Invalid: length must be positive
```

于是，`ARROW_RETURN_NOT_OK`宏可以帮你解决部分无聊的工作。他会在`Status`或`Result`为失败的时候直接返回。

```c++
std::function<arrow::Status()> test_fn = [] {
  arrow::NullBuilder builder;
  ARROW_RETURN_NOT_OK(builder.Reserve(2));
  ARROW_RETURN_NOT_OK(builder.AppendNulls(-1));
  rout << "Appended -1 null values?" << std::endl;
  return arrow::Status::OK();
};
arrow::Status st = test_fn();
rout << st << std::endl;
```

output:

```text
Invalid: length must be positive
```

### 使用Visitor模式

Arrow中的`arrow::DataType`,`arrow::Scalar`和`arrow::Array`对每种Arrow支持的类型都有一些特殊子类。为了将各个子类的逻辑抽象出来，你可以使用visitor模式。Arrow提供的内联模板函数可以帮你有效地使用visitor。

> 我觉得其本质类似于对各种类型进行不同的处理方式？

官方提供了：

- [arrow::VisitTypeInline()](https://arrow.apache.org/docs/cpp/api/utilities.html#_CPPv4I0EN5arrow15VisitTypeInlineE6StatusRK8DataTypeP7VISITOR)
- [arrow::VisitScalarInline()](https://arrow.apache.org/docs/cpp/api/utilities.html#_CPPv4I0EN5arrow17VisitScalarInlineE6StatusRK6ScalarP7VISITOR)
- [arrow::VisitArrayInline()](https://arrow.apache.org/docs/cpp/api/utilities.html#_CPPv4I0EN5arrow16VisitArrayInlineE6StatusRK5ArrayP7VISITOR)

#### 生成随机数据

> 可见[Generate Random Data for a Given Schema](https://arrow.apache.org/cookbook/cpp/create.html#generate-random-data-example)。

本文见[对已知表结构生成随机数据](#对已知表结构生成随机数据)

#### 生成跨Arrow类型的计算

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/tree/master/cpp/code_book/demo_sum)

Array visitors在写处理多种array类型的函数时很有效。但是，为每个类型实现visitor是冗余的。好消息是，Arrow提供类型traits，这允许你使用模板函数来处理不同类型的子集。下面的这个例子将会利用`arrow::enable_if_number`来证明在一个表中使用一个能对`int`和`float`类型数组的visitor即可实现求和。

```c++
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
```

```c++
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

rout << "Total is " << total;
```

output:

```text
Total is 21
```

## 创建Arrow对象

本节介绍了Arrow的Arrays、Tables、Tensors和其他Arrow实体类。

### 从STL创建Arrays

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/tree/master/cpp/code_book/arrays)

`arrow::ArrayBuilder`可以方便地使用已有的C++数据构建Arrow arrays对象：

```c++
arrow::Int32Builder builder;
ARROW_RETURN_NOT_OK(builder.Append(1));
ARROW_RETURN_NOT_OK(builder.Append(2));
ARROW_RETURN_NOT_OK(builder.Append(3));
ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, builder.Finish())
cout << arr->ToString() << std::endl;
```

> Builders会拷贝内存然后插入数据，并花费一定的时间。

Builders也可以使用STL容器：

```c++
// Raw pointers
arrow::Int64Builder long_builder = arrow::Int64Builder();
std::array<int64_t, 4> values = {1, 2, 3, 4};
ARROW_RETURN_NOT_OK(long_builder.AppendValues(values.data(), values.size()));
ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> arr, long_builder.Finish());
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
```

> `ARROW_ASSIGN_OR_RAISE`具有一定的局限性，比如操作非原子性等，可见其注释。其原理是使用std::move过去，所以注意原来的容器会失效。

### 对已知表结构生成随机数据

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/tree/master/cpp/code_book/random_data_schema)

使用`type visitor`为已知表结构生成随即数据是一种很不错的办法。下面的示例只实现了`double arrays`和`list arrays`的接口，可以拓展成其他各类接口。

```c++
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
```

然后你可以使用上述的随机数生成器来生成任意支持的表了：

```c++
arrow::Status func()
{
    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("x", arrow::float64()),
                       arrow::field("y", arrow::list(arrow::float64()))});

    RandomBatchGenerator generator(schema);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::RecordBatch> batch, generator.Generate(2));

    cout << "Created batch: " << endl
         << batch->ToString();

    // Consider using ValidateFull to check correctness
    ARROW_RETURN_NOT_OK(batch->ValidateFull());

    return arrow::Status::OK();
}
```

> 以下内容是在理解上述代码时产生的笔记：
>
> `arrow::ListArray::FromArrays(offsets, values)`的作用：将values中的值，根据offsets进行拆分，形成多个列表，上述例子中，offsets只有2个值，分别为0和一个随机数，则输出了一个列表，包含了values中下标0-随机数的值。但如果与下面例子一样，设置offsets为0,2,5，则会输出两个列表，分别包含了values中下标0-1和2-4的数值。
>
> ```c++
> arrow::Status testFromArrays()
> {
>     arrow::Int32Builder int32_builder;
>     int32_builder.Append(0);
>     int32_builder.Append(2);
>     int32_builder.Append(5);
>     std::shared_ptr<arrow::Array> offsets;
> 
>     ARROW_ASSIGN_OR_RAISE(offsets, int32_builder.Finish());
> 
>     arrow::FloatBuilder float_builder;
>     float_builder.Append(8.0);
>     float_builder.Append(7.0);
>     float_builder.Append(6.0);
>     float_builder.Append(5.0);
>     float_builder.Append(4.0);
>     float_builder.Append(3.0);
>     
>     std::shared_ptr<arrow::Array> values;
>     ARROW_ASSIGN_OR_RAISE(values, float_builder.Finish());
> 
>     ARROW_ASSIGN_OR_RAISE(auto array, arrow::ListArray::FromArrays(*offsets.get(), *values.get()));
>     cout << array->ToString() << endl;
> 
>     return arrow::Status::OK();
> }
> ```
>
> 其结果为
>
> ```text
> [
>   [
>     8,
>     7
>   ],
>   [
>     6,
>     5,
>     4
>   ]
> ]
> ```
>
> 有意思的事情是，最后一个3没有被输出，所以说明是左闭右开的。

## 读写数据集

本节包含一些读写数据集的案例，这些数据集从一个或多个表数据中提取出。

### 构造数据集

`Parquet`是一个用于复杂数据的空间效率高的列式存储格式。Parquet C++的实现是Apache Arrow项目中的一部分，因此与Arrow结合紧密。

案例混合了`docs/cpp/parquet`和`cpp/examples/parquet/parquet_stream_api`，删除了我觉得可能干扰对代码理解中的部分，使得代码我认为可读性更高一些，更便于理解。

#### 使用流读写数据

> 源码参考`cpp/examples/parquet/parquet_stream_api/stream_reader_writer.cc`
>
> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/read_write_parquet/stream.cpp)

##### 写文件

首先我们需要先确定好输出的文件名以及相关的配置信息，并构建一个`schema`：

```c++
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(
        outfile,
        arrow::io::FileOutputStream::Open("test.parquet"));
    parquet::WriterProperties::Builder builder; // 这里使用了默认配置

    std::shared_ptr<parquet::schema::GroupNode> schema; // 注意此处是parquet的schema
}
```

设定`schema`结构

```c++
/**
 * @brief 构造Schema结构
 *
 * @param schema
 */
void setSchema(std::shared_ptr<parquet::schema::GroupNode> &schema)
{
    // 函数中各个类型符合以下转换关系
    // NodeVector
    // |-- Node
    // |-- Node
    // |-- ...
    // |-- Node
    //
    // GroupNode::Make(_,_,NodeVector) 即 将NodeVector转换为GroupNode
    parquet::schema::NodeVector fields;

    // Make函数(列名, 可选项, parquet存储的类型 ,使用时需转换成的类型, 存储参数)
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "string_field", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY,
        parquet::ConvertedType::UTF8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, 1));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "char[4]_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY,
        parquet::ConvertedType::NONE, 4));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "int8_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::INT_8));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "uint16_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::UINT_16));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "int32_field", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::INT_32));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "uint64_field", parquet::Repetition::OPTIONAL, parquet::Type::INT64,
        parquet::ConvertedType::UINT_64)); // 内部以INT64存储，使用时按照UINT64使用

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));

    // User defined timestamp type.
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "timestamp_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::TIMESTAMP_MICROS));

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "chrono_milliseconds_field", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::TIMESTAMP_MILLIS));

    schema = std::static_pointer_cast<parquet::schema::GroupNode>(parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}
```

在该`schema`中，我们创建了若干列，每一列都规定了列名和格式规则。接下来我们通过流向文件中写数据：

```c++
    parquet::StreamWriter os{
        parquet::ParquetFileWriter::Open(outfile, schema, builder.build())};
    writeData(os);
```

```c++
void writeData(parquet::StreamWriter &os)
{
    char char4_array[] = "XYZ";
    int row_max = 10;
    for (int i = 0; i < row_max; ++i)
    {
        os << std::string("string_field:") + std::to_string('a' + i % 26);
        os << static_cast<char>('a' + i % 26);
        os << char4_array;
        os << static_cast<int8_t>(i % 256);
        os << static_cast<uint16_t>(10 * i);
        os << static_cast<int32_t>(-100 * i);
        os << static_cast<uint64_t>(100 * i);
        os << 1.1 * i;
        os << std::chrono::microseconds{(3 * i) * 1000000 + i}; // timestamp
        os << std::chrono::milliseconds{(3 * i) * 1000ull + i};
        os << parquet::EndRow;

        if (i == row_max / 2)
        {
            os << parquet::EndRowGroup;
        }
    }
    std::cout << "Parquet Stream Writing complete. rows: " << os.current_row() << std::endl;
}
```

于是我们就生成了一个文件`test.parquet`。

##### 读文件

读文件的操作和写一致，同样需要打开文件，定义读取数据类型，然后逐行读取：

```c++
    std::shared_ptr<arrow::io::ReadableFile> infile;

    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open("test.parquet"));
    parquet::StreamReader os{parquet::ParquetFileReader::Open(infile)};

    // 定义读取数据类型
    parquet::StreamReader::optional<std::string> opt_string; // 注意该选项可选
    char ch;
    char char_array[4];
    int8_t int8;
    uint16_t uint16;
    int32_t int32;
    parquet::StreamReader::optional<uint64_t> opt_uint64;
    double d;
    std::chrono::microseconds ts_user;
    std::chrono::milliseconds ts_ms;
```

读取数据：

```c++
    int i;
    for (i = 0; !os.eof(); ++i)
    {
        os >> opt_string;
        os >> ch;
        os >> char_array;
        os >> int8;
        os >> uint16;
        os >> int32;
        os >> opt_uint64;
        os >> d;
        os >> ts_user;
        os >> ts_ms;
        os >> parquet::EndRow;

        std::cout << *opt_string << " ";
        std::cout << ch << " ";
        std::cout << char_array << " ";
        std::cout << int8 << " ";
        std::cout << uint16 << " ";
        std::cout << int32 << " ";
        std::cout << *opt_uint64 << " ";
        std::cout << d << " ";
        std::cout << ts_user.count() << " ";
        std::cout << ts_ms.count() << " ";
        std::cout << std::endl;
    }

    std::cout << std::endl
              << "Total rows:" << i << std::endl;
```

得到输出结果:

```text
string_field:97 a XYZ  0 0 0 0 0 0 
string_field:98 b XYZ  10 -100 100 1.1 3000001 3001 
string_field:99 c XYZ  20 -200 200 2.2 6000002 6002 
string_field:100 d XYZ  30 -300 300 3.3 9000003 9003 
string_field:101 e XYZ  40 -400 400 4.4 12000004 12004 
string_field:102 f XYZ  50 -500 500 5.5 15000005 15005 
string_field:103 g XYZ  60 -600 600 6.6 18000006 18006 
string_field:104 h XYZ  70 -700 700 7.7 21000007 21007 
string_field:105 i XYZ 80 -800 800 8.8 24000008 24008 
string_field:106 j XYZ   90 -900 900 9.9 27000009 27009 
```

### 读写Arrow表数据

> 我觉得：上述的例子是我们很艰难地构建了`Schema`，并向里面流式按行插入数据。但实际上与Apache Arrow配合后，完全可以使用`arrow::Table`来代替`Schema`，二者理论上有相似的结构，且应该处于一个相同的抽象水平。
>
> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/read_write_parquet/arrow_table.cpp)

#### 写Table数据

在此，我们使用前面生成Table的办法，写一个Table声明和赋值函数：

```c++
std::shared_ptr<arrow::Table> generate_table()
{
    arrow::Int64Builder i64builder;
    for (int i = 1; i <= 5; ++i)
    {
        PARQUET_THROW_NOT_OK(i64builder.Append(i));
    }
    std::shared_ptr<arrow::Array> i64array;
    PARQUET_THROW_NOT_OK(i64builder.Finish(&i64array));

    arrow::StringBuilder strbuilder;
    PARQUET_THROW_NOT_OK(strbuilder.Append("一些"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("字符串"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("文本"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("在"));
    PARQUET_THROW_NOT_OK(strbuilder.Append("这里~"));
    std::shared_ptr<arrow::Array> strarray;
    PARQUET_THROW_NOT_OK(strbuilder.Finish(&strarray));

    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("int", arrow::int64()), arrow::field("str", arrow::utf8())});

    return arrow::Table::Make(schema, {i64array, strarray});
}
```

然后我们在外部调用，生成`Parquet`文件：

```c++
void write_parquet_file(const arrow::Table &table)
{
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

    PARQUET_ASSIGN_OR_THROW(
        outfile, arrow::io::FileOutputStream ::Open("test2.parquet", false));
    // 该函数调用的最后一个参数是parquet文件中RowGroup的大小。
    // 通常情况下，你会选择相当大的尺寸，但在本例中，我们使用一个小的值来拥有多个RowGroups。
    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(table, arrow::default_memory_pool(), outfile, 3));
}

int main(int argc, char const *argv[])
{
    std::shared_ptr<arrow::Table> table = generate_table();
    write_parquet_file(*table);
    return 0;
}
```

#### 读Table数据

读的玩法就比较多了，可以整个文件都读下来，可以只读取其中部分（按行、按列、按行列），下面的例子就是一个很不错的说明：

##### 读取整个文件

一次性读取没什么好说的，Table数据从哪儿来就回那儿去。

```c++
// #2: 读取整个文件
void read_whole_file()
{
    std::cout << std::endl
              << "一次性读取 " << PARQUET_FILE_NAME << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->ReadTable(&table));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << table->ToString() << std::endl;
    std::cout << "已加载 " << table->num_rows() << " 行，" << table->num_columns() << " 列." << std::endl;
}
```

我们可以看到与写文件大差不大。

##### 只读一个RowGroup

起初我以为RowGroup是一个文件一个RowGroup，然后一个数据集被拆分成多个数据文件，看到这里我知道我错了，Arrow在数据文件内的这么一个RowGroup的概念有点类似于我们缓存IO时用的Buffer，你可以理解成为了避免频繁请求同时避免一次性大量IO等待而采取的先读个几千字节（实际上是按行的），这也就是写文件时定义好的RowGroup。

在限定读RowGroup时，只需要一个很简单的`reader->RowGroup(0)`，即可表示要获取第一个RowGroup。当然，如果希望读多个RowGroup，可以使用`reader->ReadRowGroups({rowgroups_idxs}, &table)`，在代码中被注释了，可以手动打开。

```c++
// #3: 从文件里只读一个RowGroup
void read_single_rowgroup()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一个RowGroup" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::Table> table;
    PARQUET_THROW_NOT_OK(reader->RowGroup(0)->ReadTable(&table));
    // PARQUET_THROW_NOT_OK(reader->ReadRowGroups({0, 1}, &table));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << table->ToString() << std::endl;
    std::cout << "已加载 " << table->num_rows() << " 行，" << table->num_columns() << " 列." << std::endl;
}
```

##### 只读一列

与上面一样，可以选择一列`reader->ReadColumn(0, &array)`读取，不过我没有找到怎么读取多列，可能需要再探索一下。

```c++
// #4: 只读一列
void read_single_column()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一列" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    PARQUET_THROW_NOT_OK(reader->ReadColumn(0, &array));

    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << array->ToString() << std::endl;
    std::cout << "已加载 " << array->length() << " 行." << std::endl;
}
```

##### 只读取一个RowGroup的一列

好了，范围更小了，不再作过多解释，上代码吧：

```c++
// #5: 只读第一个RowGroup的第一列
void read_single_column_chunk()
{
    std::cout << std::endl
              << "只读取 " << PARQUET_FILE_NAME << " 中的第一个RowGroup的第一列" << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(PARQUET_FILE_NAME, arrow::default_memory_pool()));

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
    std::shared_ptr<arrow::ChunkedArray> array;
    PARQUET_THROW_NOT_OK(reader->RowGroup(0)->Column(0)->Read(&array));
    std::cout << "=== " << __func__ << " ===" << std::endl;
    std::cout << array->ToString() << std::endl;
    std::cout << "已加载 " << array->length() << " 行." << std::endl;
}
```

### 读写CSV文件

TODO 找到源码，见compute_and_write_csv_example.cc

### 读写JSON文件

TODO 非短期内重点，优先级2

## Arrow Flight

Arrow Flight是一个针对tabular数据集优化的RPC框架，建立在gRPC和IPC格式之上。

`Flight`是一个能从另一个服务下载或上传至另一个服务的框架，其是围绕Arrow记录块(? Arrow record batches)的流来组织的？

> 上面那段没有明白：Flight is organized around streams of Arrow record batches, being either downloaded from or uploaded to another service.

一组元数据方法提供了流的发现和内省，以及实现特定应用方法的能力。

> 内省：一种在运行是对类型进行判断的能力

方法和消息格式是由`Protobuf`定义的，使得与可能单独支持`gRPC`和`Arrow`但不支持`Flight`的客户端具有交互能力。然而，Flight的实现包括进一步优化了对`Protobuf`的使用方式，以避免使用Protobuf的开销（主要是避免了过多的内存拷贝）。

更多的特性在此不做过多介绍，请参考下面链接中的原文阅读。

> [原文](https://arrow.apache.org/cookbook/cpp/flight.html)
> [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)

因为官方项目中的`Flight`的Demo只提供了一个Flight Service的启动，感觉并没有什么有价值的东西，所以这次咱们还是根据`CookBook`一文中实现一个简单的Parquet落地服务吧。

### 一个简单的使用Flight的Parquet落地服务

在这个示例里，我们会实现一个服务来提供表数据的`key-value`形式的存储，然后使用`Flight`去提供上传和请求支持，使用`Parquet`去保存数据。

#### 服务端

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/flight/service.cpp)

首先，我们现实现这个服务，为了更简单地展示，我们不会用Datasets的API，而是直接使用Parquet的API。

> 在目前9.0.0版本上，使用包管理器安装的9.0.0-1版本的arrow-flight在pkgconfig中文件名与动态库对不上，需要将pc文件中`-larrow-flight`改为`-larrow_flight`。

我们首先声明一个继承自`arrow::flight::FlightServerBase`的类，构造函数入参为数据集要缓存的地址，然后声明一个`action`。

> FlightServerBase具备一些基本的接口，包括
>
> - DoPut（上传数据）
> - DoGet（获取数据）
> - DoAction（执行用户自定义操作）
> - ListActions（返回支持的操作）
> - ListFlights（返回已有的数据集）
> - .....
> 我们在上面这些接口实现了部分功能（见括号），让请求看起来像是REST一样可读。

这个`action`的动作是删除数据集文件，key为`drop_dataset`，描述是`Delete a dataset.`。

```c++
class ParquetStorageService : public arrow::flight::FlightServerBase
{
public:
    const arrow::flight::ActionType kActionDropDataset{"drop_dataset", "Delete a dataset."};
    explicit ParquetStorageService(std::shared_ptr<arrow::fs::FileSystem> root)
        : root_(std::move(root))
    {
    }

    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *,
        std::unique_ptr<arrow::flight::FlightListing> *listings) override;
    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &,
                                const arrow::flight::FlightDescriptor &descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo> *info) override;
    arrow::Status DoPut(const arrow::flight::ServerCallContext &,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter>) override;
    arrow::Status DoGet(const arrow::flight::ServerCallContext &,
                        const arrow::flight::Ticket &request,
                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override;
    arrow::Status ListActions(const arrow::flight::ServerCallContext &,
                              std::vector<arrow::flight::ActionType> *actions) override;
    arrow::Status DoAction(const arrow::flight::ServerCallContext &,
                           const arrow::flight::Action &action,
                           std::unique_ptr<arrow::flight::ResultStream> *result) override;
};
```

接下来我们按照顺序来实现功能（顺序我随心了）。

首先是ListFlights。

我们使用`filesystem`中的`FileSelector`对指定目录扫描，然后获取其中的拓展名是`parquet`的文件，然后读取信息（文件名、schema)，并组织放到`FlightInfo`中。

```c++
    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext &, const arrow::flight::Criteria *,
        std::unique_ptr<arrow::flight::FlightListing> *listings) override
    {
        arrow::fs::FileSelector selector;
        selector.base_dir = "/";
        ARROW_ASSIGN_OR_RAISE(auto listing, root_->GetFileInfo(selector));
        std::vector<arrow::flight::FlightInfo> flights;
        for (const auto &file_info : listing)
        {
            if (!file_info.IsFile() || file_info.extension() != "parquet")
                continue;

            ARROW_ASSIGN_OR_RAISE(auto info, MakeFlightInfo(file_info));
            flights.push_back(std::move(info));
        }

        *listings = std::unique_ptr<arrow::flight::FlightListing>(
            new arrow::flight::SimpleFlightListing(std::move(flights)));

        return arrow::Status::OK();
    }

    arrow::Result<arrow::flight::FlightInfo> MakeFlightInfo(
        const arrow::fs::FileInfo &file_info)
    {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(file_info));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input),
                                                     arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
        auto descriptor = arrow::flight::FlightDescriptor::Path({file_info.base_name()});
        arrow::flight::FlightEndpoint endpoint;
        endpoint.ticket.ticket = file_info.base_name();
        arrow::flight::Location location;
        ARROW_ASSIGN_OR_RAISE(location,
                              arrow::flight::Location::ForGrpcTcp("localhost", port()));
        endpoint.locations.push_back(location);

        int64_t total_records = reader->parquet_reader()->metadata()->num_rows();
        int64_t total_bytes = file_info.size();

        return arrow::flight::FlightInfo::Make(*schema, descriptor, {endpoint}, total_records,
                                               total_bytes);
    }
```

然后写GetFlightInfo，实现了获取指定数据集的信息：

```c++
    arrow::Status GetFlightInfo(const arrow::flight::ServerCallContext &,
                                const arrow::flight::FlightDescriptor &descriptor,
                                std::unique_ptr<arrow::flight::FlightInfo> *info) override
    {
        ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(descriptor));
        ARROW_ASSIGN_OR_RAISE(auto flight_info, MakeFlightInfo(file_info));
        *info = std::unique_ptr<arrow::flight::FlightInfo>(
            new arrow::flight::FlightInfo(std::move(flight_info)));

        return arrow::Status::OK();
    }

    arrow::Result<arrow::fs::FileInfo> FileInfoFromDescriptor(
        const arrow::flight::FlightDescriptor &descriptor)
    {
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH)
        {
            return arrow::Status::Invalid("Must provide PATH-type FlightDescriptor");
        }
        else if (descriptor.path.size() != 1)
        {
            return arrow::Status::Invalid(
                "Must provide PATH-type FlightDescriptor with one path component");
        }

        return root_->GetFileInfo(descriptor.path[0]);
    }
```

然后就是上传下载两件套。需要注意的是，`TableBatchReader`不能直接转成`RecordBatchStream`，因为其生命周期就在这个函数里，这也就说明，`RecordBatchStream`很可能是异步操作的。

```c++
    arrow::Status DoPut(const arrow::flight::ServerCallContext &,
                        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                        std::unique_ptr<arrow::flight::FlightMetadataWriter>) override
    {
        ARROW_ASSIGN_OR_RAISE(auto file_info, FileInfoFromDescriptor(reader->descriptor()));
        ARROW_ASSIGN_OR_RAISE(auto sink, root_->OpenOutputStream(file_info.path()));
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, reader->ToTable());
        ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
                                                       sink, /*chunk_size=*/65536));

        return arrow::Status::OK();
    }

    arrow::Status DoGet(const arrow::flight::ServerCallContext &,
                        const arrow::flight::Ticket &request,
                        std::unique_ptr<arrow::flight::FlightDataStream> *stream) override
    {
        ARROW_ASSIGN_OR_RAISE(auto input, root_->OpenInputFile(request.ticket));
        std::unique_ptr<parquet::arrow::FileReader> reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input),
                                                     arrow::default_memory_pool(), &reader));
        std::shared_ptr<arrow::Table> table;
        ARROW_RETURN_NOT_OK(reader->ReadTable(&table));

        // Note that we can't directly pass TableBatchReader to
        // RecordBatchStream because TableBatchReader keeps a non-owning
        // reference to the underlying Table, which would then get freed
        // when we exit this function

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        arrow::TableBatchReader batch_reader(*table);

        ARROW_ASSIGN_OR_RAISE(batches, batch_reader.ToRecordBatches());
        ARROW_ASSIGN_OR_RAISE(auto owning_reader, arrow::RecordBatchReader::Make(
                                                      std::move(batches), table->schema()));

        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
            new arrow::flight::RecordBatchStream(owning_reader));

        return arrow::Status::OK();
    }
```

接下来是对`actions`的显示和执行，注意，`DoAction`中对action进行了区分。

```c++
    arrow::Status ListActions(const arrow::flight::ServerCallContext &,
                              std::vector<arrow::flight::ActionType> *actions) override
    {
        *actions = {kActionDropDataset};

        return arrow::Status::OK();
    }

    arrow::Status DoAction(const arrow::flight::ServerCallContext &,
                           const arrow::flight::Action &action,
                           std::unique_ptr<arrow::flight::ResultStream> *result) override
    {
        if (action.type == kActionDropDataset.type)
        {
            *result = std::unique_ptr<arrow::flight::ResultStream>(
                new arrow::flight::SimpleResultStream({}));

            return DoActionDropDataset(action.body->ToString());
        }

        return arrow::Status::NotImplemented("Unknown action type: ", action.type);
    }

    arrow::Status DoActionDropDataset(const std::string &key)
    {
        return root_->DeleteFile(key);
    }
```

在我们构建好服务类后，我们就可以尝试启动它：

首先先清空创建一个数据集文件夹`flight_datasets`

```c++
    // 创建并清空存储的数据文件目录
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_RETURN_NOT_OK(fs->CreateDir("./flight_datasets/"));
    ARROW_RETURN_NOT_OK(fs->DeleteDirContents("./flight_datasets/"));
    auto root = std::make_shared<arrow::fs::SubTreeFileSystem>("./flight_datasets/", fs);
```

然后设置flight并初始化

```c++
    // 设置flight监听IP端口
    arrow::flight::Location server_location;
    ARROW_ASSIGN_OR_RAISE(server_location,
                          arrow::flight::Location::ForGrpcTcp("0.0.0.0", SERVER_PORT));

    // 初始化
    arrow::flight::FlightServerOptions options(server_location);
    auto server = std::unique_ptr<arrow::flight::FlightServerBase>(
        new ParquetStorageService(std::move(root)));
    ARROW_RETURN_NOT_OK(server->Init(options));
    cout << "Listening on port " << server->port() << std::endl;
```

启动服务，注意此时是阻塞的，所以我们要么创建新线程实现，要么使用另一个进程来实现客户端。

```c++
    // 启动服务（阻塞）
    ARROW_RETURN_NOT_OK(server->Serve());

    // 关闭服务
    ARROW_RETURN_NOT_OK(server->Shutdown());
```

这样一个服务端就实现了，接下来要开始实现客户端。

#### 客户端

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/flight/uploader.cpp)

客户端相对简单，主要包括了上传数据文件、查询数据文件、下载数据文件和删除数据文件的功能。与服务端的代码一一对应。

首先，我们先连上`gRPC`服务（已经将上传下载和删除入口注释）：

```c++
arrow::Status connect()
{
    arrow::flight::Location location;
    ARROW_ASSIGN_OR_RAISE(location,
                          arrow::flight::Location::ForGrpcTcp("localhost", SERVER_PORT));

    std::unique_ptr<arrow::flight::FlightClient> client;
    ARROW_ASSIGN_OR_RAISE(client, arrow::flight::FlightClient::Connect(location));
    cout << "已连接上 " << location.ToString() << std::endl;

    // ARROW_RETURN_NOT_OK(uploadData(client));
    // ARROW_RETURN_NOT_OK(getData(client));
    // ARROW_RETURN_NOT_OK(delData(client));

    client->Close();
    return arrow::Status::OK();
}
```

然后我们实现一下上传功能。

```c++
arrow::Status uploadData(std::unique_ptr<arrow::flight::FlightClient> &client);
```

首先我们打开要上传的数据文件，并构造一个reader用于读取数据

```c++
    // 打开数据文件
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::io::RandomAccessFile> input, fs->OpenInputFile(DATA_FILE_1));

    // 构造reader用于读取
    std::unique_ptr<parquet::arrow::FileReader> reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(std::move(input), arrow::default_memory_pool(), &reader));
```

然后我们为数据文件增加描述（也可以理解成请求头？）设置了PATH为文件名，然后获取了schema。

```c++
    // 设置请求头（设置文件路径和元数据）
    auto descriptor = arrow::flight::FlightDescriptor::Path({DATA_FILE_1});
    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_NOT_OK(reader->GetSchema(&schema));
```

启动了RPC上传请求，获得了`DoPutResult`，包含了reader和writer，用于数据的写入和读取反馈数据。

```c++
    // 启动RPC请求，获取writer和metadata_reader
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
    ARROW_ASSIGN_OR_RAISE(auto put_stream, client->DoPut(descriptor, schema));
    writer = std::move(put_stream.writer);
    metadata_reader = std::move(put_stream.reader);
```

然后就可以上传数据了，这里使用的是一次性全部上传，理论上可以分批循环上传，关于其是否可以多线程多进程加速暂时不知道（TODO 探索一下并行上传）

```c++
    // 上传数据
    std::shared_ptr<arrow::RecordBatchReader> batch_reader; // 创建batch读取器，一次batch包含了所有rowgroups
    std::vector<int> row_groups(reader->num_row_groups());  // 保持原有的rowgroup
    std::iota(row_groups.begin(), row_groups.end(), 0);     // 获取所有rowgroups
    cout << "row groups: 0-" << row_groups.size() - 1 << endl;

    ARROW_RETURN_NOT_OK(reader->GetRecordBatchReader(row_groups, &batch_reader));
    int64_t batches = 0;
    while (true)
    {
        ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->Next()); // 每次读取一波数据
        if (!batch)
            break;
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch)); // writer将数据写入
        batches++;
    }

    ARROW_RETURN_NOT_OK(writer->Close());
    cout << "写了 " << batches << " batches" << std::endl;
```

完成上传功能后，可以写下载功能，下载功能主要有下面的流程构成：

1. 判断是否有符合条件的flight(数据文件)，有的话输出一下结构。
2. 根据符合条件的flight获取ticket，然后读取数据。
3. 再输出一下获取的数据内容。

> 为了方便理解，官方把请求和数据比做成一架架飞机(flight)，如果需要获取指定的flight，就需要持有对应的机票(ticket)，上面第一布的目的就是获取这个ticket。

```c++
arrow::Status getData(std::unique_ptr<arrow::flight::FlightClient> &client)
{
    // 在完成写入之后，通过GetFlightInfo来获取指定descriptor文件的表结构

    auto descriptor = arrow::flight::FlightDescriptor::Path({DATA_FILE_1});

    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    ARROW_ASSIGN_OR_RAISE(flight_info, client->GetFlightInfo(descriptor));
    cout << flight_info->descriptor().ToString() << std::endl;
    cout << "=== Schema ===" << std::endl;
    std::shared_ptr<arrow::Schema> info_schema;
    arrow::ipc::DictionaryMemo dictionary_memo; // 声明从IPC到字典化的内存结构
    ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
    cout << info_schema->ToString() << std::endl;
    cout << "==============" << std::endl;

    // 然后在读取数据
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    // 有意思的是，他把flight的从目的地获取数据的过程看作坐飞机，手里需要拿个ticket，保存了目的地

    for (auto &points : flight_info->endpoints())
    {
        cout << "-----> end point:" << endl;
        cout << "ticket:" << points.ticket.ticket << endl;
        for (auto &loc : points.locations)
        {
            cout << "loc:" << loc.ToString() << endl;
        }
    }

    ARROW_ASSIGN_OR_RAISE(stream, client->DoGet(flight_info->endpoints()[0].ticket)); // 要第一个符合descriptor的文件
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, stream->ToTable());
    arrow::PrettyPrintOptions print_options(/*indent=*/0, /*window=*/2);
    ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*table, print_options, &cout));

    return arrow::Status::OK();
}
```

完成数据下载后，我们接下来实现一下自定义的action，还记得服务端我们实现了一个`drop_dataset`的action么？我们就过来请求它。

在执行action之前，我们可以看看服务支持什么action。

```c++
    // flight可以调用自定义的actions，可以先获取支持的Actions
    auto actions = client->ListActions();
    cout << "=== Actions ===" << std::endl;
    for (auto &action : actions.ValueUnsafe())
    {
        cout << "action[" << action.type << "]: " << action.description << endl;
    }
```

然后，我们就可以触发action来删除了

```c++
    // 之后我们调用支持的drop_dataset来删除DATA_FILE_1
    arrow::flight::Action action{"drop_dataset", arrow::Buffer::FromString(DATA_FILE_1)};
    std::unique_ptr<arrow::flight::ResultStream> results;
    ARROW_ASSIGN_OR_RAISE(results, client->DoAction(action));
    cout << "Deleted dataset" << DATA_FILE_1 << std::endl;
```

删除完成后验证一下，输出一下所有符合flight的shema，因为没有数据，所以直接退出了

```c++
    std::unique_ptr<arrow::flight::FlightListing> listing;
    ARROW_ASSIGN_OR_RAISE(listing, client->ListFlights()); // 获取flight列表
    while (true)
    {
        std::unique_ptr<arrow::flight::FlightInfo> flight_info;
        ARROW_ASSIGN_OR_RAISE(flight_info, listing->Next()); // 遍历一遍flight列表
        if (!flight_info)
            break;
        cout << flight_info->descriptor().ToString() << std::endl;
        cout << "=== Schema ===" << std::endl;
        std::shared_ptr<arrow::Schema> info_schema;
        arrow::ipc::DictionaryMemo dictionary_memo;
        ARROW_ASSIGN_OR_RAISE(info_schema, flight_info->GetSchema(&dictionary_memo));
        cout << info_schema->ToString() << std::endl;
        cout << "==============" << std::endl;
    }
    cout << "End of listing" << std::endl;
```

在此，我们只需要将`connect`函数中的三个注释揭开就可以使用了。

```c++
    ARROW_RETURN_NOT_OK(uploadData(client));
    ARROW_RETURN_NOT_OK(getData(client));
    ARROW_RETURN_NOT_OK(delData(client));
```

### Flight效率探索

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/tree/master/cpp/code_book/flight_speed_test)

首先简述一下我这里需要使用的效率场景，以便大家参考：

类比成交表结构，设计出以下schema：

```c++
    std::shared_ptr<arrow::Schema> schema = arrow::schema({arrow::field("sno", arrow::utf8()),
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
                                                        
```

测试环境：

- OS：Ubuntu 20.04 LTS
- CPU：Intel i5-1135G7 2.4Ghz
- RAM：15.3GB
- ROM：KBG40ZNV512G

统计信息：

数量：1000万
生成文件：4.3GB(4,321,274,275 字节) (未启用压缩)
数据生成耗时：17891.114690 ms
写文件耗时：17637.604046 ms
平均写速：233.654 MB/s

> 写单一文件存在一个致命问题：在通过grpc传输文件时，提示错误
>
> ```text
> Cannot send record batches exceeding 2GiB yet. gRPC client debug context: {"created":"@1660116194.049465350","description":"Error received from peer ipv6:[::1]:33000","file":"/build/apache-arrow-9.0.0/cpp_build/grpc_ep-prefix/src/grpc_ep/src/core/lib/surface/call.cc","file_line":952,"grpc_message":"Cannot send record batches exceeding 2GiB yet","grpc_status":3}. Client context: OK
> ```
>
> 这就意味着如果要传输大文件，可能必须要引入类似hdfs的机制对文件分区。或者找寻什么办法，将一个batch拆分成多个小batch来传输。不幸的是，2GB的限制好像并不能通过设置调整。

不幸的消息：并没有找到合适的能拆分batch的办法。这意味着没有办法直接在ArrowFlight上使用超过2GB的数据。并且我没有找到什么合适的办法获取Table或RecordBatch的字节（因为数据可能不连续，所以要么手动解析拷贝到一整块连续内存中，要么实现一个reader，在链表跳转读取。），再加上GRPC对大数据的传输效率并不高，所以我们可能不得不放弃这种办法，转而注意数据的拆分传输。

不过我还是找到了一个[官方示例](https://arrow.apache.org/docs/cpp/examples/row_columnar_conversion.html)，这个示例让我们实现了`Table`和一个`struct data_row`的互相转换，这与Table中的Array结构是不同的，前者是实值，而后者全部是指针，或许可以通过这种方式将数据塞到grpc，然后再组装成一个Table，不过可能会比较复杂，且可行性未知，关键是效率并不清楚是否能做到足够好。因此暂且搁置，等未来真的需要的时候再考虑。 TODO。

那这样就意味着我们接下来需要探索Arrow数据的处理。

## Arrow数据操作

前面只是介绍了数据从哪儿来的和怎么传输的，接下来会介绍如何使用这部分数据，这也是Arrow最主要的立足之本。

在使用之前，先说明一下Arrow Datasets库提供了处理表结构的数据和那些大于内存的多文件数据集的功能，包括了

- 统一的接口
- 数据发现能力
- 通过过滤、映射等方式提高读取效率

先来一个简单的例子，我们构建一个小型的数据集，之后的操作都使用了这个数据集。

```c++
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
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
```

### 数据文件发现能力

这一节，我们会基于一两个例子来介绍一下arrow的数据文件发现能力，这得益于arrow针对多种数据实现了统一的接口。因此，我们可以使用arrow实现读取不同类型的数据集和不同分区的数据集。

#### 多Parquet数据文件读写

这个例子将数据集五五开拆分成两个`parquet`文件（并非分区），然后读取两个文件所在的数据目录。

##### 将数据写入多个Parque文件

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/make_parquet.cpp)

```c++
/**
 * @brief 生成一个由两个数据文件组成的数据集
 *
 * @param filesystem fs
 * @param root_path 根目录
 * @return arrow::Result<std::string> 数据集地址
 */
arrow::Result<std::string> CreateExampleParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem, const std::string &root_path)
{
    auto base_path = root_path + "/" + DATASET_NAME + "_output/parquet_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));         // 创建一个数据文件夹
    ARROW_RETURN_NOT_OK(filesystem->DeleteDirContents(base_path)); // 删除文件夹内历史数据
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());              // 创建表

    // 写入两个文件里，每个文件五行
    ARROW_ASSIGN_OR_RAISE(auto output,
                          filesystem->OpenOutputStream(base_path + "/data1.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table->Slice(0, 5), arrow::default_memory_pool(), output, /*chunk_size=*/2048));
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table->Slice(5), arrow::default_memory_pool(), output, /*chunk_size=*/2048));
    return base_path;
}
```

执行后，会生成一个`slice_output/parquet_dataset`目录，目录里有两个`parquet`文件。

##### 发现并读取多个parquet文件

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/scan_whole_datasets.cpp)

`arrow::dataset::dataset`对象可以通过各种 arrow::dataset::datasetFactory对象来创建。在这里，我们将使用 arrow::dataset::FileSystemDatasetFactory，它可以在给定的基础目录路径下读取一个数据集。

为了方便同步，我们在`common.h`里定义了一些全局变量，并且赋予了一些默认值，对代码可读性可能会产生一定干扰，所以阅读时请务必明确该变量是只使用默认值还是当作变量使用。

我们实现一个扫描函数，该函数可以在指定目录下扫描符合`format`条件的数据文件`fragment`。并将每个`fragment`输出并合并读取。

```c++
/**
 * @brief 扫描整个指定目录，获取数据类型、路径和数据表对象
 *
 * @param filesystem
 * @param format 需要扫描的文件格式
 * @param base_dir
 * @return arrow::Result<std::shared_ptr<arrow::Table>>
 */
arrow::Result<std::shared_ptr<arrow::Table>> ScanWholeDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
    const std::shared_ptr<arrow::dataset::FileFormat> &format, const std::string &base_dir)
{
    // 通过扫描路径获取dataset
    // 我们也要传递要使用的文件系统和要用于读取的文件格式。这让我们可以选择（例如）读取本地文件或Amazon S3中的文件，或在Parquet和CSV之间进行选择。
    arrow::fs::FileSelector selector;
    selector.base_dir = base_dir;
    ARROW_ASSIGN_OR_RAISE(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format, arrow::dataset::FileSystemFactoryOptions()));
    ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());

    // 输出fragments，一个fragments可以代表一个数据集块？
    ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments())
    for (const auto &fragment : fragments)
    {
        std::cout << "发现 fragment: " << (*fragment)->ToString() << std::endl;
    }

    // 读取整个路径下的数据文件，并放到一张Table里
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}
```

最终我们可以发现，一共读到了10行，说明哪怕是分割成两个文件，也完整地读了出来。

```text
Read 10 rows
```

在这里，官方提示，这种方式会占用大量的内存，数量取决于数据集大小。

有意思的是，我们可以关注一下table的`ToString`结果：

```text
a: int64
b: int64
c: int64
----
a:
  [
    [
      0,
      1,
      2,
      3,
      4
    ],
    [
      5,
      6,
      7,
      8,
      9
    ]
  ]
b:
 ...(省略号)...
c:
 ...(省略号)...
```

我们发现，a下面的列表（暂且可以这么理解）包含了两个子列表，刚好五五开，符合生成代码中`Slice`的设定，而且在我们此前创建的数据文件中，我们发现一个完整文件的一列是类似这样的：

```text
a:
  [
    [
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ]
  ]
```

所以这可能就是拆分文件的实现原理？等我们在后面使用分区读写文件后，我们再观察一下。

为了确定`format`过滤条件是否和文件名后缀有关（上面的例子我们创建的是以`.parquet`结尾的文件），我们来一个恶作剧，将生成文件的文件名后缀改成`.trick`，然后我们再看看能否得到我们想要的结果：

```c++
    ARROW_ASSIGN_OR_RAISE(auto output,
                          filesystem->OpenOutputStream(base_path + "/data1.trick"));
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.trick"));
```

奇妙的事情发生了，我们依旧能扫描到结果，所以我猜测与后缀名无关，可能与文件的`metadata`相关。不知道在csv或者json文件是否也能做到这一点。

##### 将数据写入多个Feather文件

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/make_feather.cpp)

Feather文件我并不怎么了解，根据官方代码说明，感觉可能是和IPC有关的一种协议结构。这里再次展示统一的接口有多爽，在统一的Factory下，可以选择不同的生成方式，这次使用的是`IpcFileFormat`。

我们只需要在生成`parquet`的源码基础上修改几个配置即可：（IPC的schema需要手动说明）

```c++
format = std::make_shared<arrow::dataset::IpcFileFormat>();
```

```c++
    ARROW_ASSIGN_OR_RAISE(auto output,
                          filesystem->OpenOutputStream(base_path + "/data1.feather"));
    ARROW_ASSIGN_OR_RAISE(auto writer,
                          arrow::ipc::MakeFileWriter(output.get(), table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table->Slice(0, 5)));
    ARROW_RETURN_NOT_OK(writer->Close());
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.feather"));
    ARROW_ASSIGN_OR_RAISE(writer,
                          arrow::ipc::MakeFileWriter(output.get(), table->schema()));
    ARROW_RETURN_NOT_OK(writer->WriteTable(*table->Slice(5)));
    ARROW_RETURN_NOT_OK(writer->Close());
```

##### 发现并读取多个Feather文件

这个就更简单了，只需要将`ScanWholeDataset`的参数改一下就好（源码见[发现并读取多个parquet文件](#发现并读取多个parquet文件)，按照注释改一下就好）

```c++
    // format = std::make_shared<arrow::dataset::ParquetFileFormat>(); // Parquet
    format = std::make_shared<arrow::dataset::IpcFileFormat>(); // IPC

    // std::string base_path{root_path + "/" + DATASET_NAME + "_output/parquet_dataset"}; // Parquet
    std::string base_path{root_path + "/" + DATASET_NAME + "_output/feather_dataset"}; // IPC
```

##### 对指定列设定读取方式

在scan前，可以对scan的方式进行设置，但这部分我没成功，也没找到有相关的示例。

```c++
    // 可以设置读取方式？ TODO 没明白为啥没作用
    // auto options = std::make_shared<arrow::dataset::ParquetFragmentScanOptions>();
    // options->arrow_reader_properties->set_read_dictionary(0, true); // 第一行是dict
    // ARROW_RETURN_NOT_OK(scan_builder->FragmentScanOptions(options));
```

#### 过滤数据

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/filter_select_dataset.cpp)

目前我们都只是读所有的数据，在前面也有只读一列或者读一定数量的`recordgroup`，下面会说明使用`Scanner`来控制读哪些数据。

在这个例子里，我们会用`arrow::dataset::ScannerBuilder::Project()`来控制读哪一列。

```c++
/**
 * @brief 过滤的方式读取数据
 *
 * @param filesystem
 * @param format
 * @param base_dir
 * @return arrow::Result<std::shared_ptr<arrow::Table>>
 */
arrow::Result<std::shared_ptr<arrow::Table>> FilterAndSelectDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
    const std::shared_ptr<arrow::dataset::FileFormat> &format, const std::string &base_dir)
{
    // 扫描获取文件
    arrow::fs::FileSelector selector;
    selector.base_dir = base_dir;
    ARROW_ASSIGN_OR_RAISE(
        auto factory, arrow::dataset::FileSystemDatasetFactory::Make(filesystem, selector, format,
                                                                     arrow::dataset::FileSystemFactoryOptions()));
    ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());

    // 只读特定的列 b ，并限制行条件为小于4
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
    ARROW_RETURN_NOT_OK(scan_builder->Project({"b"}));                                                                           // 设置只读b列
    ARROW_RETURN_NOT_OK(scan_builder->Filter(arrow::compute::less(arrow::compute::field_ref("b"), arrow::compute::literal(4)))); // 条件设置为b<4
    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}
```

注释能看的很清楚了，不过我想再试试，如果Project不为b会发生什么：

```c++
ARROW_RETURN_NOT_OK(scan_builder->Project({"c"})); // 读个c列
```

实际上也能正常使用，并且得到了c的输出，数量也一致，大概率是条件也是生效的。

#### 列表映射

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/project_dataset.cpp)

`arrow::dataset::ScannerBuilder::Project()`除了能够选择列获取数据，也可以实现比较复杂的映射功能，例如重命名列名、列类型转换、基于表达式构建新列等等。

在这个例子里，我们会用一系列表达式去构建列表名和值，包括：

- 对a列重命名为`a_renamed`
- 对b列修改为`float32`类型，并重命名为`b_as_float32`
- 对c列修改为`boolean`类型，并重命名为`c_1`

```c++
    // ProjectDataset函数
    ARROW_RETURN_NOT_OK(scan_builder->Project({
                                                  // 直接取列a
                                                  arrow::compute::field_ref("a"),
                                                  // 列b转换为float32
                                                  arrow::compute::call("cast", {arrow::compute::field_ref("b")},
                                                                       arrow::compute::CastOptions::Safe(arrow::float32())),
                                                  // 把c按照c==1设为布尔值
                                                  arrow::compute::equal(arrow::compute::field_ref("c"), arrow::compute::literal(1)),
                                              },
                                              // 列名
                                              {"a_renamed", "b_as_float32", "c_1"}));
```

output:

```text
Read 10 rows
a_renamed: int64
b_as_float32: float
c_1: bool
----
a_renamed:
  [
    [
      0,
      1,
      2,
      3,
      4
    ],
    [
      5,
      6,
      7,
      8,
      9
    ]
  ]
b_as_float32:
  [
    [
      9,
      8,
      7,
      6,
      5
    ],
    [
      4,
      3,
      2,
      1,
      0
    ]
  ]
c_1:
  [
    [
      true,
      false,
      true,
      false,
      true
    ],
    [
      false,
      true,
      false,
      true,
      false
    ]
  ]
```

在这种模式下，只有指定的列才会出现在结果表中。

如果你想在现有的列之外再包含一个派生列，则可以从数据集中建立表达式：

```c++
    // SelectAndProjectDataset函数
    std::vector<std::string> names;                // 列名
    std::vector<arrow::compute::Expression> exprs; // 表达式
    // 读取现在所有的列名
    for (const auto &field : dataset->schema()->fields())
    {
        names.push_back(field->name());
        exprs.push_back(arrow::compute::field_ref(field->name()));
    }
    // 构建一个新列，新列的值是b>1的布尔值
    names.emplace_back("b_large");
    exprs.push_back(arrow::compute::greater(arrow::compute::field_ref("b"), arrow::compute::literal(1)));
```

output:

```text
Read 10 rows
a: int64
b: int64
c: int64
b_large: bool
----
a:
  [
    [
      0,
      1,
      2,
      3,
      4
    ],
    [
      5,
      6,
      7,
      8,
      9
    ]
  ]
b:
  [
    [
      9,
      8,
      7,
      6,
      5
    ],
    [
      4,
      3,
      2,
      1,
      0
    ]
  ]
c:
  [
    [
      1,
      2,
      1,
      2,
      1
    ],
    [
      2,
      1,
      2,
      1,
      2
    ]
  ]
b_large:
  [
    [
      true,
      true,
      true,
      true,
      true
    ],
    [
      true,
      true,
      true,
      false,
      false
    ]
  ]
```

值的注意的是，当过滤器和投影结合使用的时候，Arrow会确定所有需要读取的列。假如有一个列没有被选入结果表中，但它出现在了过滤条件中，Arrow仍会读取该列以用于过滤数据。

#### 读写分区数据集

目前为止，我们一直在处理由扁平目录和文件所组成的数据集。但通常情况下，一个数据集会有一个或多个经常被过滤的列。通过将文件组成一个嵌套的目录结构，我们可以定义一个分区的数据集，其中子目录的名称包含了关于数据的哪个子集存储在该目录中的信息，而不是必须读取然后过滤数据。然后，我们可以通过使用这种信息来避免加载不符合过滤器的文件，从而更有效地过滤数据。

##### 生成分区数据集

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/make_partition_parquet.cpp)

一个例子，下面展示了一个由年月拆分的数据集结构：

```text
dataset_name/
  year=2007/
    month=01/
       data0.parquet
       data1.parquet
       ...
    month=02/
       data0.parquet
       data1.parquet
       ...
    month=03/
    ...
  year=2008/
    month=01/
    ...
  ...
```

上述分区策略使用了`Apache Hive`使用的`key=value`字典存储列名。在这种规则下，位于`dataset_name/year=2007/month=01/data0.parquet`只保存了2007年1月的数据。

首先我们先构建一套小一点的便于理解的数据：

它比咱们之前的数据多了一列`part`字段。

```c++
arrow::Result<std::shared_ptr<arrow::Table>> CreateTableWithPart()
{
    auto schema = arrow::schema(
        {arrow::field("a", arrow::int64()),
         arrow::field("b", arrow::int64()),
         arrow::field("c", arrow::int64()),
         arrow::field("part", arrow::utf8())});
    std::vector<std::shared_ptr<arrow::Array>> arrays(4);
    arrow::NumericBuilder<arrow::Int64Type> builder;

    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_RETURN_NOT_OK(builder.Finish(&arrays[0]));
    builder.Reset();

    ARROW_RETURN_NOT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&arrays[1]));
    builder.Reset();

    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&arrays[2]));

    arrow::StringBuilder string_builder;
    ARROW_RETURN_NOT_OK(
        string_builder.AppendValues({"a", "a", "a", "a", "a", "b", "b", "b", "b", "b"}));
    ARROW_RETURN_NOT_OK(string_builder.Finish(&arrays[3]));

    return arrow::Table::Make(schema, arrays);
}
```

然后我们使用dataset来写文件（而不是直接写入）：

```c++
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTableWithPart());      // 创建表(多了一个part字段)

    // 通过dataset来写文件
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);
    ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());
```

然后我们配置一下分区策略，我们设置以part字段实现`key=value`的目录结构。目录内文件以`part{i}.parquet`为命名规则生成数据文件。

```c++
    // partition schema说明了要按照哪个列分区，这里取part列
    auto partition_schema = arrow::schema({arrow::field("part", arrow::utf8())});
    // 我们使用Hive-style分区方式，这种方式会创建key=value形式的目录结构
    auto partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    // 写parquet文件
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    arrow::dataset::FileSystemDatasetWriteOptions write_options;
    write_options.file_write_options = format->DefaultWriteOptions();
    write_options.filesystem = filesystem;
    write_options.base_dir = base_path;
    write_options.partitioning = partitioning;
    write_options.basename_template = "part{i}.parquet"; // 文件名为part{i}.parquet
```

调用`FileSystemDataset::Write`并传入策略和数据扫描器

```c++
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options, scanner));
```

运行后，我们可以得到想要的数据集：

```text
.
└── parquet_dataset
    ├── part=a
    │   └── part0.parquet
    └── part=b
        └── part0.parquet
```

##### 读取分区数据集

> 代码在 [此处跳转](https://github.com/ZhengqiaoWang/ArrowDocsZhCN/blob/master/cpp/code_book/arrow_operators/scan_partition_datasets.cpp)

扫描分区数据集与上面扫描多文件数据集有相似之处，在工厂类和配置上使用略有不同。

与此同时，我们在获取数据时，只获取`part=b`的数据，这样就意味着不会读取另一个`part=a`目录下的文件。

```c++
/**
 * @brief 获取分区数据集
 *
 * @param filesystem
 * @param format
 * @param base_dir
 * @return arrow::Result<std::shared_ptr<arrow::Table>>
 */
arrow::Result<std::shared_ptr<arrow::Table>> ScanPartitionedDataset(
    const std::shared_ptr<arrow::fs::FileSystem> &filesystem,
    const std::shared_ptr<arrow::dataset::FileFormat> &format, const std::string &base_dir)
{
    arrow::fs::FileSelector selector;
    selector.base_dir = base_dir;
    selector.recursive = true; // 确保扫描子目录
    arrow::dataset::FileSystemFactoryOptions options;
    // 使用Hive-style分区方式。我们让Arrow Dataset推断出分区方式
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();
    ARROW_ASSIGN_OR_RAISE(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(
                                            filesystem, selector, format, options));
    ARROW_ASSIGN_OR_RAISE(auto dataset, factory->Finish());

    // 输出fragments
    ARROW_ASSIGN_OR_RAISE(auto fragments, dataset->GetFragments());
    for (const auto &fragment : fragments)
    {
        std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
        std::cout << "Partition expression: "
                  << (*fragment)->partition_expression().ToString() << std::endl;
    }
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());

    // 我们使用filter过滤一些条件，下面的条件是取part=b的数据，这也就意味着，不会读取part!=b的文件。
    ARROW_RETURN_NOT_OK(scan_builder->Filter(arrow::compute::equal(arrow::compute::field_ref("part"), arrow::compute::literal("b"))));

    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}
```

### 计算函数

> 原文在 [此处跳转](https://arrow.apache.org/docs/cpp/compute.html)

计算是`Arrow`的一项关键能力，利用其计算能力能有效地处理各类数据。

本节我将不会严格的按照官方文档说明，而是挑选几个我认为比较常用的计算函数，写出示例，帮助大家理解。

不过官方文档提到的一些应该提前知晓的内容也会尽可能全的体现在本节中。

#### 简介

计算函数可以有不同类型的输入。在其内部，一个函数由一个或几个“内核(`kernels`)”实现，这取决于具体的输入类型（例如，一个函数将两个输入的值相加，可以有不同的内核，这取决于输入的是整数还是浮点数）。

> 我认为，`Arrow`中的`kernel`内核含义应该指的是针对不同类型在该计算方法中的实现。

各个计算函数被注册在一个全局的`FunctionRegistry`类中，你可以通过名称来查找这些计算函数。

于是我们可以通过`GetFunctionNames`输出默认和自定义计算函数

```c++
    auto registry = cp::GetFunctionRegistry();
    for (auto &name : registry->GetFunctionNames())
    {
        cout << name << ", ";
    }
```

我们可以看到他支持了相当多的默认函数：

```text
abs, abs_checked, acos, acos_checked, add, add_checked, all, and, and_kleene, and_not, and_not_kleene, any, approximate_median, array_filter, array_sort_indices, array_take, ascii_capitalize, ascii_center, ascii_is_alnum, ascii_is_alpha, ascii_is_decimal, ascii_is_lower, ascii_is_printable, ascii_is_space, ascii_is_title, ascii_is_upper, ascii_lower, ascii_lpad, ascii_ltrim, ascii_ltrim_whitespace, ascii_reverse, ascii_rpad, ascii_rtrim, ascii_rtrim_whitespace, ascii_split_whitespace, ascii_swapcase, ascii_title, ascii_trim, ascii_trim_whitespace, ascii_upper, asin, asin_checked, assume_timezone, atan, atan2, binary_join, binary_join_element_wise, binary_length, binary_repeat, binary_replace_slice, binary_reverse, bit_wise_and, bit_wise_not, bit_wise_or, bit_wise_xor, case_when, cast, ceil, ceil_temporal, choose, coalesce, cos, cos_checked, count, count_distinct, count_substring, count_substring_regex, cumulative_sum, cumulative_sum_checked, day, day_of_week, day_of_year, day_time_interval_between, days_between, dictionary_encode, divide, divide_checked, drop_null, ends_with, equal, extract_regex, fill_null_backward, fill_null_forward, filter, find_substring, find_substring_regex, floor, floor_temporal, greater, greater_equal, hash_all, hash_any, hash_approximate_median, hash_count, hash_count_distinct, hash_distinct, hash_list, hash_max, hash_mean, hash_min, hash_min_max, hash_one, hash_product, hash_stddev, hash_sum, hash_tdigest, hash_variance, hour, hours_between, if_else, index, index_in, index_in_meta_binary, indices_nonzero, invert, is_dst, is_finite, is_in, is_in_meta_binary, is_inf, is_leap_year, is_nan, is_null, is_valid, iso_calendar, iso_week, iso_year, less, less_equal, list_element, list_flatten, list_parent_indices, list_value_length, ln, ln_checked, log10, log10_checked, log1p, log1p_checked, log2, log2_checked, logb, logb_checked, make_struct, map_lookup, match_like, match_substring, match_substring_regex, max, max_element_wise, mean, microsecond, microseconds_between, millisecond, milliseconds_between, min, min_element_wise, min_max, minute, minutes_between, mode, month, month_day_nano_interval_between, month_interval_between, multiply, multiply_checked, nanosecond, nanoseconds_between, negate, negate_checked, not_equal, or, or_kleene, partition_nth_indices, power, power_checked, product, quantile, quarter, quarters_between, random, rank, replace_substring, replace_substring_regex, replace_with_mask, round, round_temporal, round_to_multiple, second, seconds_between, select_k_unstable, shift_left, shift_left_checked, shift_right, shift_right_checked, sign, sin, sin_checked, sort_indices, split_pattern, split_pattern_regex, sqrt, sqrt_checked, starts_with, stddev, strftime, string_is_ascii, strptime, struct_field, subsecond, subtract, subtract_checked, sum, take, tan, tan_checked, tdigest, true_unless_null, trunc, unique, us_week, us_year, utf8_capitalize, utf8_center, utf8_is_alnum, utf8_is_alpha, utf8_is_decimal, utf8_is_digit, utf8_is_lower, utf8_is_numeric, utf8_is_printable, utf8_is_space, utf8_is_title, utf8_is_upper, utf8_length, utf8_lower, utf8_lpad, utf8_ltrim, utf8_ltrim_whitespace, utf8_normalize, utf8_replace_slice, utf8_reverse, utf8_rpad, utf8_rtrim, utf8_rtrim_whitespace, utf8_slice_codeunits, utf8_split_whitespace, utf8_swapcase, utf8_title, utf8_trim, utf8_trim_whitespace, utf8_upper, value_counts, variance, week, weeks_between, xor, year, year_month_day, years_between
```
