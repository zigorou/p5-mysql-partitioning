use inc::Module::Install;
name 'MySQL-Partitioning';
all_from 'lib/MySQL/Partitioning.pm';

requires 'parent';
requires 'Data::Util';
requires 'DBD::mysql';
requires 'DBI';
requires 'Carp';
requires 'Class::Accessor::Fast';
requires 'File::Which';
requires 'Scalar::Util';
requires 'SQL::Abstract::Limit';
requires 'String::ShellQuote';
requires 'Try::Tiny';

tests_recursive 't';
author_tests 'xt';

test_requires 'SQL::SplitStatement';
test_requires 'Test::Exception';
test_requires 'Test::LoadAllModules';
test_requires 'Test::More';
test_requires 'Test::Requires';
test_requires 'Test::Skip::UnlessExistsExecutable';

auto_set_repository;
auto_include;
WriteAll;
