#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[],
    std::string& r_name,
    std::string& start_date,
    std::string& end_date,
    int& num_threads,
    std::string& table_path,
    std::string& result_path) {

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--r_name" && i + 1 < argc)
            r_name = argv[++i];

        else if (arg == "--start_date" && i + 1 < argc)
            start_date = argv[++i];

        else if (arg == "--end_date" && i + 1 < argc)
            end_date = argv[++i];

        else if (arg == "--threads" && i + 1 < argc)
            num_threads = std::stoi(argv[++i]);

        else if (arg == "--table_path" && i + 1 < argc)
            table_path = argv[++i];

        else if (arg == "--result_path" && i + 1 < argc)
            result_path = argv[++i];
    }

    // DEBUG PRINT (important)
    std::cout << "Parsed values:\n";
    std::cout << "r_name: " << r_name << "\n";
    std::cout << "start_date: " << start_date << "\n";
    std::cout << "end_date: " << end_date << "\n";
    std::cout << "threads: " << num_threads << "\n";
    std::cout << "table_path: " << table_path << "\n";
    std::cout << "result_path: " << result_path << "\n";

    return !r_name.empty() &&
           !start_date.empty() &&
           !end_date.empty() &&
           num_threads > 0 &&
           !table_path.empty() &&
           !result_path.empty();
}

std::vector<std::map<std::string, std::string>> readTable(const std::string& file) {
    std::vector<std::map<std::string, std::string>> data;
    std::ifstream fin(file);
    std::string line;

    while (getline(fin, line)) {
        std::stringstream ss(line);
        std::string token;
        std::map<std::string, std::string> row;

        std::vector<std::string> cols;
        while (getline(ss, token, '|')) {
            cols.push_back(token);
        }

        // store generic columns
        for (int i = 0; i < cols.size(); i++) {
            row[std::to_string(i)] = cols[i];
        }

        data.push_back(row);
    }

    return data;
}
// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path,
    std::vector<std::map<std::string, std::string>>& customer_data,
    std::vector<std::map<std::string, std::string>>& orders_data,
    std::vector<std::map<std::string, std::string>>& lineitem_data,
    std::vector<std::map<std::string, std::string>>& supplier_data,
    std::vector<std::map<std::string, std::string>>& nation_data,
    std::vector<std::map<std::string, std::string>>& region_data) {

    customer_data = readTable(table_path + "/customer.tbl");
    orders_data = readTable(table_path + "/orders.tbl");
    lineitem_data = readTable(table_path + "/lineitem.tbl");
    supplier_data = readTable(table_path + "/supplier.tbl");
    nation_data = readTable(table_path + "/nation.tbl");
    region_data = readTable(table_path + "/region.tbl");

    return true;
}


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results) {

    std::mutex mtx;

    auto worker = [&](int start, int end) {
        std::map<std::string, double> local;

        for (int i = start; i < end; i++) {
            auto& l = lineitem_data[i];

            std::string orderkey = l.at("0");
            std::string suppkey = l.at("2");
            double price = std::stod(l.at("5"));
            double discount = std::stod(l.at("6"));

            for (auto& o : orders_data) {
                if (o.at("0") != orderkey) continue;

                std::string date = o.at("4");
                if (date < start_date || date >= end_date) continue;

                std::string custkey = o.at("1");

                for (auto& c : customer_data) {
                    if (c.at("0") != custkey) continue;

                    std::string nationkey = c.at("3");

                    for (auto& s : supplier_data) {
                        if (s.at("0") != suppkey) continue;
                        if (s.at("3") != nationkey) continue;

                        for (auto& n : nation_data) {
                            if (n.at("0") != nationkey) continue;

                            std::string regionkey = n.at("2");

                            for (auto& r : region_data) {
                                if (r.at("0") == regionkey && r.at("1") == r_name) {

                                    std::string nation = n.at("1");
                                    double revenue = price * (1 - discount);
                                    local[nation] += revenue;
                                }
                            }
                        }
                    }
                }
            }
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (auto& p : local) {
            results[p.first] += p.second;
        }
    };

    std::vector<std::thread> threads;
    int chunk = lineitem_data.size() / num_threads;

    for (int i = 0; i < num_threads; i++) {
        int start = i * chunk;
        int end = (i == num_threads - 1) ? lineitem_data.size() : start + chunk;
        threads.emplace_back(worker, start, end);
    }

    for (auto& t : threads) t.join();

    return true;
}
// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream fout(result_path + "/output.txt");

    for (auto& r : results) {
        fout << r.first << " : " << r.second << std::endl;
        std::cout << r.first << " : " << r.second << std::endl;
    }

    return true;
}