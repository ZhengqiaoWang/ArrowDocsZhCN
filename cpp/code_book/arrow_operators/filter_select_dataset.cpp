
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

#include <iostream>
using namespace std;

#include "common.h"

#define DATASET_NAME "slice"

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
    // ARROW_RETURN_NOT_OK(scan_builder->Project({"b"}));                                                                           // 设置只读b列
    ARROW_RETURN_NOT_OK(scan_builder->Project({"c"})); // 读个c列

    ARROW_RETURN_NOT_OK(scan_builder->Filter(arrow::compute::less(arrow::compute::field_ref("b"), arrow::compute::literal(4)))); // 条件设置为b<4
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

    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, FilterAndSelectDataset(fs, format, base_path));

    std::cout << "Read " << table->num_rows() << " rows" << std::endl;
    std::cout << table->ToString() << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
