#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <vector>
#include <map>
#include <unordered_map>
// ======================= PARSE ARGS =======================
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

    return !r_name.empty() &&
           !start_date.empty() &&
           !end_date.empty() &&
           num_threads > 0 &&
           !table_path.empty() &&
           !result_path.empty();
}

// ======================= READ TABLE =======================
std::vector<std::map<std::string, std::string>> readTable(const std::string& file) {
    std::vector<std::map<std::string, std::string>> data;
    std::ifstream fin(file);

    if (!fin.is_open()) {
        std::cerr << "Error opening file: " << file << std::endl;
        return data;
    }

    std::string line;
    while (getline(fin, line)) {
        std::stringstream ss(line);
        std::string token;
        std::map<std::string, std::string> row;

        std::vector<std::string> cols;
        while (getline(ss, token, '|')) {
            cols.push_back(token);
        }

        for (int i = 0; i < cols.size(); i++) {
            row[std::to_string(i)] = cols[i];
        }

        data.push_back(row);
    }

    return data;
}


// ======================= READ ALL DATA =======================
// ======================= READ ALL DATA =======================
bool readTPCHData(const std::string& table_path,
                  std::vector<std::map<std::string, std::string>>& customer_data,
                  std::vector<std::map<std::string, std::string>>& orders_data,
                  std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::vector<std::map<std::string, std::string>>& supplier_data,
                  std::vector<std::map<std::string, std::string>>& nation_data,
                  std::vector<std::map<std::string, std::string>>& region_data) {

    // Read all tables directly from table_path
    customer_data = readTable(table_path + "/customer.tbl");
    if (customer_data.empty()) { std::cerr << "Failed to read customer.tbl\n"; return false; }

    orders_data = readTable(table_path + "/orders.tbl");
    if (orders_data.empty()) { std::cerr << "Failed to read orders.tbl\n"; return false; }

    lineitem_data = readTable(table_path + "/lineitem.tbl");
    if (lineitem_data.empty()) { std::cerr << "Failed to read lineitem.tbl\n"; return false; }

    supplier_data = readTable(table_path + "/supplier.tbl");
    if (supplier_data.empty()) { std::cerr << "Failed to read supplier.tbl\n"; return false; }

    nation_data = readTable(table_path + "/nation.tbl");
    if (nation_data.empty()) { std::cerr << "Failed to read nation.tbl\n"; return false; }

    region_data = readTable(table_path + "/region.tbl");
    if (region_data.empty()) { std::cerr << "Failed to read region.tbl\n"; return false; }

    std::cout << "All TPC-H tables loaded successfully from " << table_path << std::endl;

    return true;
}

// ======================= QUERY 5 =======================
bool executeQuery5(
    const std::string& r_name,
    const std::string& start_date,
    const std::string& end_date,
    int num_threads,
    const std::vector<std::map<std::string, std::string>>& customer_data,
    const std::vector<std::map<std::string, std::string>>& orders_data,
    const std::vector<std::map<std::string, std::string>>& lineitem_data,
    const std::vector<std::map<std::string, std::string>>& supplier_data,
    const std::vector<std::map<std::string, std::string>>& nation_data,
    const std::vector<std::map<std::string, std::string>>& region_data,
    std::map<std::string, double>& results)
{
    std::mutex mtx;

    // =================== BUILD LOOKUP MAPS ===================
    std::map<std::string, std::string> order_to_cust;
    for (const auto& o : orders_data) {
        std::string orderkey = o.at("0");
        std::string date = o.at("4");
        if (date >= start_date && date <= end_date)
            order_to_cust[orderkey] = o.at("1");
    }

    std::map<std::string, std::string> cust_to_nation;
    for (const auto& c : customer_data) {
        cust_to_nation[c.at("0")] = c.at("3");
    }

    std::map<std::string, std::string> supp_to_nation;
    for (const auto& s : supplier_data) {
        supp_to_nation[s.at("0")] = s.at("3");
    }

    std::map<std::string, std::string> nation_to_region;
    std::map<std::string, std::string> nation_to_name;
    for (const auto& n : nation_data) {
        nation_to_region[n.at("0")] = n.at("2");
        nation_to_name[n.at("0")] = n.at("1");
    }

    std::map<std::string, std::string> region_names;
    for (const auto& r : region_data) {
        region_names[r.at("0")] = r.at("1");
    }
    // ==========================================================

    auto worker = [&](int start, int end) {
        std::map<std::string, double> local;

        for (int i = start; i < end; ++i) {
            const auto& l = lineitem_data[i];
            std::string orderkey = l.at("0");
            std::string suppkey = l.at("2");
            double price = std::stod(l.at("5"));
            double discount = std::stod(l.at("6"));

            auto o_it = order_to_cust.find(orderkey);
            if (o_it == order_to_cust.end()) continue;

            std::string custkey = o_it->second;

            std::string nationkey = cust_to_nation[custkey];

            if (supp_to_nation[suppkey] != nationkey) continue;

            std::string regionkey = nation_to_region[nationkey];
            if (region_names[regionkey] != r_name) continue;

            std::string nation = nation_to_name[nationkey];
            double revenue = price * (1 - discount);
            local[nation] += revenue;
        }

        std::lock_guard<std::mutex> lock(mtx);
        for (auto& p : local) results[p.first] += p.second;
    };

    int chunk = (lineitem_data.size() + num_threads - 1) / num_threads;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        int start = i * chunk;
        int end = std::min((int)lineitem_data.size(), start + chunk);
        threads.emplace_back(worker, start, end);
    }

    for (auto& t : threads) t.join();

    return true;
}
// ======================= OUTPUT =======================
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream fout(result_path + "/output.txt");

    std::vector<std::pair<std::string, double>> vec(results.begin(), results.end());

    std::sort(vec.begin(), vec.end(), [](auto& a, auto& b) {
        return b.second > a.second;
    });

    for (auto& r : vec) {
        fout << r.first << " : " << r.second << std::endl;
        std::cout << r.first << " : " << r.second << std::endl;
    }

    return true;
}