#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

#include "gmtime.h"

using namespace std;

typedef unsigned int uint32;
typedef unsigned long long uint64;
typedef signed long long int64;

enum class ColumnType : uint32 { TEXT, BIGINT, FLOAT, DATE, TIMESTAMP, NULLTYPE, RIDCOL };

typedef tuple<string, ColumnType> QualifiedColumn;

vector<string> projectColNames(const vector<QualifiedColumn>& qcols) {
    vector<string> ret;
    for (auto c : qcols) {
        ret.push_back(get<0>(c));
    }
    return std::move(ret);
}

vector<ColumnType> projectColTypes(const vector<QualifiedColumn>& qcols) {
    vector<ColumnType> ret;
    for (auto c : qcols) {
        ret.push_back(get<1>(c));
    }
    return std::move(ret);
}

enum class ListNum : uint32 {
    firstNameSet,
    lastNameSet,

};

using namespace std::chrono;

system_clock::time_point tp_epoch; // epoch value

string print_date(int64 value) noexcept {
    stringstream ss;

    try {
        using namespace std::chrono;
        auto x = tp_epoch + days(value);
        std::time_t x_c = std::chrono::system_clock::to_time_t(x);
        std::tm my_time;
        auto ret = GMTIME(&my_time, &x_c);
        if (ret != 0) {
            ss << "error";
        } else {
            ss << std::put_time(&my_time, "%Y-%m-%d");
        }
    } catch (...) {
        ss << "exception";
    }

    return ss.str();
}

tuple<int64, int64> get_timeinfo_from_millis(int64 value) {
    int64 secs_from_epoch = value / 1000;
    int64 millis = value % 1000;
    return make_tuple(secs_from_epoch, millis);
}

std::locale my_locale;

string print_timestamp(int64 value) noexcept {
    stringstream ss;

    try {
        using namespace std::chrono;

        int64 secs_from_epoch, millis;
        std::tie(secs_from_epoch, millis) = get_timeinfo_from_millis(value);

        auto x = tp_epoch + seconds(secs_from_epoch);
        std::time_t x_c = std::chrono::system_clock::to_time_t(x);
        std::tm my_time;
        auto ret = GMTIME(&my_time, &x_c);
        if (ret != 0) {
            ss << "error";
        } else {
            stringstream ss_local;
            ss_local << std::put_time(&my_time, "%Y-%m-%dT%H:%M:%S");
            if (millis > 0) {
                ss_local << "." << millis;
            }

            ss << ss_local.str();
        }
    } catch (...) {
        ss << "exception";
    }

    return ss.str();
}

const vector<QualifiedColumn> columns{{"fullname", ColumnType::TEXT},
                                      {"id", ColumnType::BIGINT}, // auto
                                      {"dob", ColumnType::DATE},
                                      {"employeedate", ColumnType::DATE},
                                      {"manager", ColumnType::TEXT},    // TAG
                                      {"department", ColumnType::TEXT}, // TAG
                                      {"height", ColumnType::FLOAT},
                                      {"lastseen", ColumnType::TIMESTAMP},
                                      {"numkids", ColumnType::BIGINT}};

int main(int argc, char* argv[]) {
    if ((argc < 3) || (argc > 4)) {
        cout << "Usage:  datacreator num-of-rows  outfilename_base  [seed]\n";
        return -1;
    }

    uint64 num_rows = std::atol(argv[1]);

    string outname(argv[2]);

    unsigned seed = (argc >= 4) ? atoi(argv[3]) : (unsigned)reinterpret_cast<uint64>(main);

    if (num_rows == 0) {
        cout << "Usage:  datacreator num-of-rows  outfilename_base  [seed]\n";
        return -1;
    }

    const vector<string> first_names = {"Niels", "Harold", "Erwin", "Max", "Miguel", "Albert", "David", "Alice", "Bob", "Enrico", "Juan", "Richard"};
    const vector<string> last_names = {
        "Bohr", "White", "Schrodinger", "Planck", "Einstein", "Alcubierre", "K", "Feynman", "De Broglie", "Fermi", "Maldacena", "Sanfilippo", "Z3"};

    vector<string> colnames = projectColNames(columns);
    vector<ColumnType> coltypes = projectColTypes(columns);

    const vector<string> tags2 = {"HR", "Sales", "Marketing", "Engineering", "Product", "Psyco", "Other"};

    uint64 id = 1000;

    ofstream hset(outname + ".txt", std::ofstream::out);
    ofstream csv(outname + ".csv", std::ofstream::out);

    csv << "pkey,";

    for (auto col : columns) {
        csv << get<0>(col) << ",";
    }

    csv << endl;
    srand(seed);

    for (uint64 idx = 1; idx <= num_rows; idx++) {
        vector<string> row;
        uint64 temp = rand();
        int sign = ((temp % 2) == 0) ? +1 : -1;

        row.push_back(first_names[temp % first_names.size()] + " " + last_names[rand() % last_names.size()]);
        row.push_back(print_date(temp % 20000));
        row.push_back(print_date(rand() % 20000));
        row.push_back(string("\"") + (first_names[rand() % first_names.size()]) + " " + last_names[rand() % last_names.size()] + "\"");
        row.push_back(tags2[temp % tags2.size()]);
        row.push_back(to_string(1.85 + sign * rand()));
        row.push_back(print_timestamp(temp * 10000001ull + rand() * 10000));
        row.push_back(to_string(rand() % 5));

        // auto id = (temp * uint64(rand()));

        hset << "HSET " << outname << ":" << idx;
        hset << " fullname \"" << row[0] << "\"";
        hset << " id " << idx;
        hset << " dob " << row[1];
        hset << " employeedate " << row[2];
        hset << " manager " << row[3];
        hset << " department " << row[4];
        hset << " height " << row[5];
        hset << " lastupdate \"" << row[6] << "\"";
        hset << " numkids " << row[7];
        hset << endl;

        csv << outname << ":" << idx;
        csv << "," << row[0];
        csv << "," << idx;
        csv << "," << row[1];
        csv << "," << row[2];
        csv << "," << row[3];
        csv << "," << row[4];
        csv << "," << row[5];
        csv << "," << row[6];
        csv << "," << row[7];
        csv << "," << endl;
    }

    return 0;
}
