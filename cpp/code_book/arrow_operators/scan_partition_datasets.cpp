
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

#define DATASET_NAME "partition"

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

arrow::Status func()
{
    std::shared_ptr<arrow::dataset::FileFormat> format;
    format = std::make_shared<arrow::dataset::ParquetFileFormat>(); // Parquet
    // format = std::make_shared<arrow::dataset::IpcFileFormat>(); // IPC

    std::string base_path{root_path + "/" + DATASET_NAME + "_output/parquet_dataset"}; // Parquet
    // std::string base_path{root_path + "/" + DATASET_NAME + "_output/feather_dataset"}; // IPC

    ARROW_ASSIGN_OR_RAISE(auto fs, arrow::fs::FileSystemFromUri(uri, &root_path));

    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, ScanPartitionedDataset(fs, format, base_path));

    std::cout << "Read " << table->num_rows() << " rows" << std::endl;
    std::cout << table->ToString() << std::endl;
    return arrow::Status::OK();
}

int main(int argc, char const *argv[])
{
    cout << func() << endl;
    return 0;
}
