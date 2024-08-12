#include "locale_set.hpp"

#include <iostream>
#include <string>

class thousands_sep : public std::numpunct<char>
{
    char do_thousands_sep() const noexcept override { return ','; }
    std::string do_grouping() const noexcept override { return "\3"; }
};

void configure_locale(std::ostream &out, bool setGlobal) noexcept
{
    static const thousands_sep punct{};
    std::locale newLoc{ out.getloc(), &punct };
    out.imbue(newLoc);
    if (setGlobal) { std::locale::global(newLoc); }
}