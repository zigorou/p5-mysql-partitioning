use inc::Module::Install;
name 'MySQL-Partitioning';
all_from 'lib/MySQL/Partitioning.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
