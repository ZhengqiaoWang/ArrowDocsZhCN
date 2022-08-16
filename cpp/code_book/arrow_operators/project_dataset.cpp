
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/filesystem/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/discovery.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_ipc.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/scanner.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/expression.h>

#include <iostream>
using namespace std;

#include "common.h"

#define DATASET_NAME "slice"

/**
 * @brief 使用映射方式读取数据集
 *
 * @param filesystem
 * @param format
 * @param base_dir
 * @return arrow::Result<std::shared_ptr<arrow::Table>>
 */
arrow::Result<std::shared_ptr<arrow::Table>> ProjectDataset(
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

    // 映射获取数据集
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
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

    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}

arrow::Result<std::shared_ptr<arrow::Table>> SelectAndProjectDataset(
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

    // 映射获取数据集
    ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
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

    ARROW_RETURN_NOT_OK(scan_builder->Project(exprs, names));
    ARROW_ASSIGN_OR_RAISE(auto scanner, scan_builder->Finish());
    return scanner->ToTable();
}

arrow::Status func()
{
    std::shared_ptr<arrow::dataset::FileFormat> format;
    format = std::make_shared<arrow::dataset::ParquetFileFormat>(); // Parquet
    // format = std::make_shared<arrow::dataset::IpcFileFormat>(); // IPC

    std::string base_path{root_path + "/" + DATASET_NAME + "_output/parquet_dataset"}; // Parquet
    // std::string base_path{root_path + "/" + DATASET_NAME + "_output/feather_dataset"}; // IPC

    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &root_path));

    {
        std::shared_ptr<arrow::Table> table;
        ARROW_ASSIGN_OR_RAISE(table, ProjectDataset(fs, format, base_path));

        std::cout << "Read " << table->num_rows() << " rows" << std::endl;
        std::cout << table->ToString() << std::endl;
    }

    {
        std::shared_ptr<arrow::Table> table;
        ARROW_ASSIGN_OR_RAISE(table, SelectAndProjectDataset(fs, format, base_path));

        std::cout << "Read " << table->num_rows() << " rows" << std::endl;
        std::cout << table->ToString() << std::endl;
    }
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
