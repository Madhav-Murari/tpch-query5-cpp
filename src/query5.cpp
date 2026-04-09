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
#include <unordered_set>
#include <string>
#include <iomanip>
#include <chrono>

using namespace std;

struct Region   { int r_regionkey; string r_name; };
struct Nation   { int n_nationkey; string n_name; int n_regionkey; };
struct Supplier { int s_suppkey;   int s_nationkey; };
struct Customer { int c_custkey;   int c_nationkey; };
struct Order    { int o_orderkey;  int o_custkey;   string o_orderdate; };
struct LineItem { int l_orderkey;  int l_suppkey;
                  double l_extendedprice; double l_discount; };

static vector<Region>   g_regions;
static vector<Nation>   g_nations;
static vector<Supplier> g_suppliers;
static vector<Customer> g_customers;
static vector<Order>    g_orders;
static vector<LineItem> g_lineitems;

static void splitPipe(const string& line, vector<string>& out) {
    out.clear();
    size_t start = 0, pos;
    while ((pos = line.find('|', start)) != string::npos) {
        out.emplace_back(line, start, pos - start);
        start = pos + 1;
    }
    if (start < line.size()) out.emplace_back(line, start);
}

bool parseArgs(int argc, char* argv[],
               string& r_name, string& start_date, string& end_date,
               int& num_threads, string& table_path, string& result_path) {
    for (int i = 1; i < argc; i++) {
        string arg = argv[i];
        if      (arg == "--r_name"      && i+1 < argc) r_name      = argv[++i];
        else if (arg == "--start_date"  && i+1 < argc) start_date  = argv[++i];
        else if (arg == "--end_date"    && i+1 < argc) end_date    = argv[++i];
        else if (arg == "--threads"     && i+1 < argc) num_threads = stoi(argv[++i]);
        else if (arg == "--table_path"  && i+1 < argc) table_path  = argv[++i];
        else if (arg == "--result_path" && i+1 < argc) result_path = argv[++i];
    }
    return !r_name.empty() && !start_date.empty() && !end_date.empty()
           && num_threads > 0 && !table_path.empty() && !result_path.empty();
}

bool readTPCHData(const string& table_path,
                  vector<map<string,string>>&,
                  vector<map<string,string>>&,
                  vector<map<string,string>>&,
                  vector<map<string,string>>&,
                  vector<map<string,string>>&,
                  vector<map<string,string>>&) {

    auto t0 = chrono::high_resolution_clock::now();
    vector<string> c;

    { ifstream f(table_path + "/region.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 2) g_regions.push_back({stoi(c[0]), c[1]}); } }

    { ifstream f(table_path + "/nation.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 3) g_nations.push_back({stoi(c[0]), c[1], stoi(c[2])}); } }

    { ifstream f(table_path + "/supplier.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 4) g_suppliers.push_back({stoi(c[0]), stoi(c[3])}); } }

    { ifstream f(table_path + "/customer.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 4) g_customers.push_back({stoi(c[0]), stoi(c[3])}); } }

    { ifstream f(table_path + "/orders.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 5) g_orders.push_back({stoi(c[0]), stoi(c[1]), c[4]}); } }

    g_lineitems.reserve(12000000);
    { ifstream f(table_path + "/lineitem.tbl"); string line;
      while (getline(f, line)) { if (line.empty()) continue; splitPipe(line, c);
        if (c.size() >= 7) g_lineitems.push_back({stoi(c[0]), stoi(c[2]),
                                                   stod(c[5]), stod(c[6])}); } }

    double ms = chrono::duration<double,milli>(
                    chrono::high_resolution_clock::now()-t0).count();
    cout << "Loaded: "
         << g_regions.size() << " regions, " << g_nations.size() << " nations, "
         << g_suppliers.size() << " suppliers, " << g_customers.size() << " customers, "
         << g_orders.size() << " orders, " << g_lineitems.size() << " lineitems\n"
         << fixed << setprecision(0) << "Load time: " << ms << " ms\n";

    if (g_regions.empty() || g_nations.empty() || g_suppliers.empty() ||
        g_customers.empty() || g_orders.empty() || g_lineitems.empty()) {
        cerr << "One or more tables failed to load.\n"; return false;
    }
    return true;
}

bool executeQuery5(
    const string& r_name, const string& start_date, const string& end_date,
    int num_threads,
    const vector<map<string,string>>&, const vector<map<string,string>>&,
    const vector<map<string,string>>&, const vector<map<string,string>>&,
    const vector<map<string,string>>&, const vector<map<string,string>>&,
    map<string,double>& results)
{
    // ================= FIND REGION =================
    int target_rk = -1;
    for (const auto& r : g_regions) {
        if (r.r_name == r_name) {
            target_rk = r.r_regionkey;
            break;
        }
    }
    if (target_rk == -1) {
        cerr << "Region not found\n";
        return false;
    }

    // ================= FIND MAX KEYS =================
    int max_orderkey = 0, max_custkey = 0, max_suppkey = 0, max_nationkey = 0;

    for (auto& o : g_orders) max_orderkey = max(max_orderkey, o.o_orderkey);
    for (auto& c : g_customers) max_custkey = max(max_custkey, c.c_custkey);
    for (auto& s : g_suppliers) max_suppkey = max(max_suppkey, s.s_suppkey);
    for (auto& n : g_nations) max_nationkey = max(max_nationkey, n.n_nationkey);

    // ================= CREATE FAST ARRAYS =================
    vector<int> cust_nation(max_custkey + 1, -1);
    vector<int> supp_nation(max_suppkey + 1, -1);
    vector<int> order_nation(max_orderkey + 1, -1);
    vector<string> nation_name(max_nationkey + 1);

    // ================= FILTER NATIONS =================
    unordered_set<int> valid_nations;
    for (const auto& n : g_nations) {
        if (n.n_regionkey == target_rk) {
            valid_nations.insert(n.n_nationkey);
            nation_name[n.n_nationkey] = n.n_name;
        }
    }

    // ================= BUILD LOOKUPS =================
    for (const auto& s : g_suppliers)
        supp_nation[s.s_suppkey] = s.s_nationkey;

    for (const auto& c : g_customers)
        if (valid_nations.count(c.c_nationkey))
            cust_nation[c.c_custkey] = c.c_nationkey;

    for (const auto& o : g_orders) {
        if (o.o_orderdate < start_date || o.o_orderdate >= end_date)
            continue;

        int nk = cust_nation[o.o_custkey];
        if (nk != -1)
            order_nation[o.o_orderkey] = nk;
    }

    // ================= MULTITHREAD PROCESS =================
    size_t total = g_lineitems.size();
    size_t chunk = (total + num_threads - 1) / num_threads;

    vector<vector<double>> local_results(num_threads, vector<double>(max_nationkey + 1, 0.0));
    vector<thread> threads;

    auto worker = [&](int tid, size_t lo, size_t hi) {
        auto& local = local_results[tid];

        for (size_t i = lo; i < hi; ++i) {
            const auto& li = g_lineitems[i];

            int nk = order_nation[li.l_orderkey];
            if (nk == -1) continue;

            if (supp_nation[li.l_suppkey] != nk) continue;

            local[nk] += li.l_extendedprice * (1.0 - li.l_discount);
        }
    };

    for (int t = 0; t < num_threads; ++t) {
        size_t lo = t * chunk;
        size_t hi = min(lo + chunk, total);

        threads.emplace_back(worker, t, lo, hi);
    }

    for (auto& th : threads) th.join();

    // ================= MERGE RESULTS =================
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i <= max_nationkey; ++i) {
            if (local_results[t][i] > 0)
                results[nation_name[i]] += local_results[t][i];
        }
    }

    return true;
}
bool outputResults(const string& result_path, const map<string,double>& results) {
    vector<pair<string,double>> vec(results.begin(), results.end());
    sort(vec.begin(), vec.end(),
         [](const auto& a, const auto& b){ return a.second > b.second; });

    ofstream fout(result_path + "/output.txt");
    if (!fout) { cerr << "Cannot write output.txt\n"; return false; }

    cout << "\n" << left << setw(30) << "n_name"
         << right << setw(22) << "revenue" << "\n" << string(52,'-') << "\n";
    for (const auto& r : vec) {
        cout << left << setw(30) << r.first
             << right << fixed << setprecision(4) << setw(22) << r.second << "\n";
        fout << r.first << " : " << fixed << setprecision(4) << r.second << "\n";
    }
    cout << "\nResults written to: " << result_path << "/output.txt\n";
    return true;
}