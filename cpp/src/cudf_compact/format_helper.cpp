
#include "cudf_compact/format_helper.hpp"

#include <iostream>
#include <locale>
#include <string>

class thousands_sep : public std::numpunct<char>
{
    char do_thousands_sep() const noexcept override {
        return ',';
    }
    std::string do_grouping() const noexcept override {
        return "\3";
    }
};

std::lock_guard<std::mutex> lockGuard() noexcept {
    static std::mutex lock;
    return std::lock_guard{ lock };
}

// NOT thread safe!
void switchLocale() noexcept {
    static thousands_sep punct{};
    static std::locale spare{ std::locale(), &punct };
    // swap the global locale
    spare = std::locale::global(spare);
}
