#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>  // <-- added for timing

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;

    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }

    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;

    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }


    std::map<std::string, double> results;

    // ======================= START TIMER =======================
    auto start_time = std::chrono::high_resolution_clock::now();
    // ===========================================================

    if (!executeQuery5(
            r_name,
            "1992-01-01",   // wider start date
            "1998-12-31",   // wider end date
            num_threads,
            customer_data,
            orders_data,
            lineitem_data,
            supplier_data,
            nation_data,
            region_data,
            results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }

    // ======================= STOP TIMER ========================
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    std::cout << "Query 5 execution time: " << duration << " ms" << std::endl;
    // ===========================================================

    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    std::cout << "TPCH Query 5 implementation completed." << std::endl;
    return 0;
}