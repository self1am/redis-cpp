#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <sstream>
#include <chrono>

const int BUFFER_SIZE = 1024;

// Structure to hold value and expiry time
struct ValueWithExpiry {
    std::string value;
    std::chrono::time_point<std::chrono::steady_clock> expiry;
    bool has_expiry;
    
    ValueWithExpiry(const std::string& val) : 
        value(val), has_expiry(false) {}
    
    ValueWithExpiry(const std::string& val, long px_millis) : 
        value(val), 
        expiry(std::chrono::steady_clock::now() + std::chrono::milliseconds(px_millis)),
        has_expiry(true) {}
        
    bool is_expired() const {
        if (!has_expiry) return false;
        return std::chrono::steady_clock::now() > expiry;
    }
};

// Global key-value store with mutex for thread safety
std::unordered_map<std::string, ValueWithExpiry> kv_store;
std::mutex kv_mutex;

// Parse RESP array and return vector of strings
std::vector<std::string> parse_resp(const std::string& input) {
    std::vector<std::string> parts;
    std::istringstream iss(input);
    std::string line;
    
    // First line should be array length
    std::getline(iss, line);
    if (line[0] != '*') return parts;
    
    int array_len = std::stoi(line.substr(1));
    
    for (int i = 0; i < array_len; i++) {
        // Get bulk string length
        std::getline(iss, line);
        if (line[0] != '$') continue;
        
        // Get actual string
        std::getline(iss, line);
        parts.push_back(line);
    }
    
    return parts;
}

// Convert string to uppercase for case-insensitive comparison
std::string to_upper(const std::string& str) {
    std::string upper = str;
    for (char& c : upper) {
        c = toupper(c);
    }
    return upper;
}

// Handle different commands
std::string handle_command(const std::vector<std::string>& parts) {
    if (parts.empty()) return "-ERR empty command\r\n";
    
    std::string command = to_upper(parts[0]);
    
    if (command == "PING") {
        return "+PONG\r\n";
    }
    else if (command == "ECHO" && parts.size() > 1) {
        return "+" + parts[1] + "\r\n";
    }
    else if (command == "SET" && parts.size() >= 2) {
        std::lock_guard<std::mutex> lock(kv_mutex);
        
        // Check for PX argument
        if (parts.size() >= 4 && to_upper(parts[3]) == "PX" && parts.size() >= 5) {
            try {
                long px_millis = std::stol(parts[4]);
                kv_store[parts[1]] = ValueWithExpiry(parts[2], px_millis);
            } catch (const std::exception& e) {
                return "-ERR invalid expire time in 'set' command\r\n";
            }
        } else {
            kv_store[parts[1]] = ValueWithExpiry(parts[2]);
        }
        return "+OK\r\n";
    }
    else if (command == "GET" && parts.size() > 1) {
        std::lock_guard<std::mutex> lock(kv_mutex);
        auto it = kv_store.find(parts[1]);
        if (it != kv_store.end()) {
            if (!it->second.is_expired()) {
                return "+" + it->second.value + "\r\n";
            }
            // Remove expired key
            kv_store.erase(it);
        }
        return "$-1\r\n"; // Redis null response
    }
    
    return "-ERR unknown command\r\n";
}

void handle_client(int client_fd) {
    char buffer[BUFFER_SIZE];
    std::string incomplete_data;
    
    while (true) {
        std::memset(buffer, 0, BUFFER_SIZE);
        ssize_t bytes_read = recv(client_fd, buffer, BUFFER_SIZE - 1, 0);
        
        if (bytes_read <= 0) {
            std::cout << "Client disconnected\n";
            break;
        }
        
        incomplete_data += std::string(buffer, bytes_read);
        
        // Process all complete commands in the buffer
        size_t pos;
        while ((pos = incomplete_data.find("\r\n")) != std::string::npos) {
            std::string command = incomplete_data.substr(0, pos + 2);
            incomplete_data = incomplete_data.substr(pos + 2);
            
            std::vector<std::string> parts = parse_resp(command);
            std::string response = handle_command(parts);
            send(client_fd, response.c_str(), response.length(), 0);
        }
    }
    close(client_fd);
}

int main(int argc, char **argv) {
    // Main function remains unchanged
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    std::cout << "Waiting for clients to connect...\n";

    std::vector<std::thread> client_threads;

    while (true) {
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "Failed to accept client connection\n";
            continue;
        }

        std::cout << "Client connected\n";
        
        client_threads.emplace_back(handle_client, client_fd);
        client_threads.back().detach();
    }

    close(server_fd);
    return 0;
}