// prime_generator_v2_7.cpp

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/random.hpp>
#include <iostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <random>
#include <csignal>
#include <chrono>
#include <ctime>
#include <sstream>
#include <atomic>

using namespace boost::multiprecision;
using namespace std;

std::mutex output_mutex;
std::mutex file_mutex;
std::mutex queue_mutex;
std::condition_variable queue_cv;
bool stop_flag = false;
std::atomic<bool> found_prime_flag = false;
std::atomic<int> current_digits{ 1 };

std::atomic<int> total_primes_found{ 0 };
std::atomic<int64_t> total_test_time_ms{ 0 };
std::atomic<int> total_tests{ 0 };

const char* VERSION = "2.7";
const char* CSV_FILENAME = "primes.csv";
const int MAX_THREADS = 64;

queue<pair<cpp_int, int>> task_queue; // number and digit length

// ------------------- Prime Utilities -------------------

vector<uint32_t> small_primes;

void build_small_primes(uint32_t limit = 10000) {
    vector<bool> is_prime(limit + 1, true);
    is_prime[0] = is_prime[1] = false;

    for (uint32_t i = 2; i <= limit; ++i) {
        if (is_prime[i]) {
            small_primes.push_back(i);
            for (uint32_t j = i * 2; j <= limit; j += i)
                is_prime[j] = false;
        }
    }
}

cpp_int pow10(int digits) {
    cpp_int result = 1;
    for (int i = 0; i < digits; ++i)
        result *= 10;
    return result;
}

cpp_int generate_random_number_with_digits(int digits, mt19937& gen) {
    if (digits < 1) digits = 1;

    cpp_int lower = pow10(digits - 1);
    cpp_int upper = pow10(digits) - 1;

    boost::random::uniform_int_distribution<cpp_int> dist(lower, upper);
    return dist(gen);
}

std::string shorten_number(const cpp_int& n, size_t max_len = 12) {
    std::string s = n.str();
    if (s.size() <= max_len) return s;
    return s.substr(0, 6) + "..." + s.substr(s.size() - 6) + " (len:" + to_string(s.size()) + ")";
}

std::string current_timestamp() {
    auto now = chrono::system_clock::now();
    time_t t = chrono::system_clock::to_time_t(now);
    tm buf;
    localtime_s(&buf, &t);
    char output[100];
    strftime(output, sizeof(output), "%Y-%m-%d %H:%M:%S", &buf);
    return string(output);
}

cpp_int modpow(cpp_int base, cpp_int exponent, const cpp_int& modulus) {
    cpp_int result = 1;
    base %= modulus;
    while (exponent > 0) {
        if ((exponent & 1) != 0)
            result = (result * base) % modulus;
        base = (base * base) % modulus;
        exponent >>= 1;
    }
    return result;
}

bool millerRabinTest(cpp_int d, const cpp_int& n, const cpp_int& a) {
    cpp_int x = modpow(a, d, n);
    if (x == 1 || x == n - 1) return true;

    while (d != n - 1) {
        x = (x * x) % n;
        d <<= 1;
        if (x == 1) {
            cerr << "[Miller-Rabin] FAIL (base " << a << ") — x became 1 during squaring.\n";
            cerr << "  → n = " << n << "\n";
            return false;
        }
        if (x == n - 1) return true;
    }

    cerr << "[Miller-Rabin] FAIL (base " << a << ") — loop ended without x == n - 1.\n";
    cerr << "  → n = " << n << "\n";
    return false;
}

bool isPrime(const cpp_int& n, int rounds, std::string& reason) {
    if (n < 2) {
        reason = "Less than 2";
        return false;
    }
    if (n == 2 || n == 3) {
        reason = "2 or 3";
        return true;
    }
    if (n % 2 == 0) {
        reason = "Even number";
        return false;
    }

    for (uint32_t p : small_primes) {
        if (n == p) {
            reason = "Small prime";
            return true;
        }
        if (n % p == 0) {
            reason = "Divisible by " + std::to_string(p);
            return false;
        }
    }

    if (n < 100000000) {
        reason = "Passed trial division (small number)";
        return true;
    }

    cpp_int d = n - 1;
    while ((d & 1) == 0) d >>= 1;

    cpp_int a = 2;
    if (!millerRabinTest(d, n, a)) {
        reason = "Failed Miller-Rabin test";
        return false;
    }

    reason = "Probably prime";
    return true;
}

void save_state(int digits) {
    std::lock_guard<std::mutex> lock(file_mutex);
    std::ofstream ofs("state.txt", std::ios::trunc);
    if (ofs.is_open()) {
        ofs << digits << std::endl;
    }
}

int load_state() {
    std::lock_guard<std::mutex> lock(file_mutex);
    std::ifstream ifs("state.txt");
    int digits = 1;
    if (ifs.is_open()) {
        ifs >> digits;
        if (digits < 1) digits = 1;
    }
    return digits;
}

// ------------------- Producer -------------------

void producer() {
    int digits = load_state();
    const int max_digits = 1000000;
    random_device rd;
    mt19937 gen(rd());

    while (!stop_flag) {
        cpp_int n = generate_random_number_with_digits(digits, gen);

        {
            lock_guard<mutex> lock(queue_mutex);
            task_queue.push({ n, digits });
        }
        queue_cv.notify_one();

        this_thread::sleep_for(chrono::seconds(2));

        if (found_prime_flag.exchange(false)) {
            digits = 1;
        }
        else {
            if (digits < max_digits) digits++;
        }

        current_digits = digits;
    }

    save_state(digits);
}

// ------------------- Consumer -------------------

void consumer(int thread_id, int rounds) {
    while (!stop_flag) {
        pair<cpp_int, int> job;
        {
            unique_lock<mutex> lock(queue_mutex);
            queue_cv.wait(lock, [] { return !task_queue.empty() || stop_flag; });
            if (stop_flag) return;
            job = task_queue.front();
            task_queue.pop();
        }
        queue_cv.notify_all();

        cpp_int n = job.first;
        int digits = job.second;
        std::string reason;

        auto start = chrono::steady_clock::now();
        bool prime = isPrime(n, rounds, reason);
        auto end = chrono::steady_clock::now();
        auto duration_ms = chrono::duration_cast<chrono::milliseconds>(end - start).count();

        total_test_time_ms += duration_ms;
        total_tests++;
        if (prime) total_primes_found++;

        std::string shortened = shorten_number(n);
        std::string result = prime ? "PRIME" : "COMPOSITE";
        std::string timestamp = current_timestamp();

        {
            lock_guard<std::mutex> lock(file_mutex);
            ofstream csv(CSV_FILENAME, ios::app);
            if (csv.is_open()) {
                csv << timestamp << "," << thread_id << "," << digits
                    << "," << shortened << "," << result << "," << reason
                    << "," << duration_ms << "\n";
            }
        }

        {
            lock_guard<std::mutex> lock(output_mutex);
            cout << "[Thread " << thread_id << "] " << result << ": " << n
                << " (Digits: " << digits << ", Time: " << duration_ms << " ms)\n";
            cout << "Reason: " << reason << "\n";
        }
    }
}

// ------------------- Signal Handling -------------------

void signalHandler(int signum) {
    cout << "\nSIGINT received. Stopping...\n";
    stop_flag = true;
    queue_cv.notify_all();
    save_state(current_digits.load());
}

// ------------------- Main -------------------

int main(int argc, char* argv[]) {
    signal(SIGINT, signalHandler);

    int num_threads = thread::hardware_concurrency();
    if (num_threads <= 0 || num_threads > MAX_THREADS) num_threads = 4;
    int rounds = 10;

    if (argc > 1) num_threads = atoi(argv[1]);
    if (argc > 2) rounds = atoi(argv[2]);

    build_small_primes();

    {
        ifstream test(CSV_FILENAME);
        if (!test.good()) {
            ofstream csv(CSV_FILENAME);
            csv << "Timestamp,ThreadID,Digits,Number,Result,Reason,TimeMs\n";
        }
    }

    cout << "Prime Generator v" << VERSION << " (Producer-Consumer Model)\n";
    cout << "Threads: " << num_threads << ", Rounds: " << rounds << "\n";
    cout << "Logging to: " << CSV_FILENAME << "\n";

    thread prod(producer);

    vector<thread> threads;
    for (int i = 0; i < num_threads - 1; ++i)
        threads.emplace_back(consumer, i, rounds);

    prod.join();
    for (auto& t : threads)
        t.join();

    save_state(current_digits.load());

    int tests = total_tests.load();
    int64_t time_ms = total_test_time_ms.load();
    int primes = total_primes_found.load();
    double avg_time = (tests > 0) ? (double)time_ms / tests : 0.0;

    cout << "\n--- Summary ---\n";
    cout << "Total Primes Found: " << primes << "\n";
    cout << "Total Tests: " << tests << "\n";
    cout << "Average Time: " << avg_time << " ms\n";

    ofstream summary("summary.txt");
    if (summary.is_open()) {
        summary << "Prime Generator Summary\n";
        summary << "=======================\n";
        summary << "Total Primes Found: " << primes << "\n";
        summary << "Total Tests: " << tests << "\n";
        summary << "Average Time: " << avg_time << " ms\n";
    }

    cout << "Shutdown complete.\n";
    return 0;
}
