# file-as-of-now


#!/usr/bin/perl
use strict;
use warnings;

# Settings
my $TABLE_NAME = "nasdaq_prices";
my $DATABASE_ENGINE = "InnoDB";
my $DEFAULT_CHARSET = "latin1";
my $filename = "prices.csv";

# File Handles
open(my $table_fh, ">", "mysqlCreateSchema.sql") or die "Failed to open schema output file";
open(my $values_fh, ">", "mysqlInsertValues.sql") or die "Failed to open insert output file";
open(my $file_fh, "<", $filename) or die "Failed to open input CSV: $!";

# Read column names
my $header = <$file_fh>;
chomp($header);
$header =~ s/["']//g;
$header =~ s/\r|\n//g;
$header =~ s/ /_/g;

my @Field_Names = split(",", $header);
my $Field_Names_Count = scalar @Field_Names;

# Prepare for type inference
my @type;
my @length;
my @decimal_length1;
my @decimal_length2;

my $row_count = 0;

while (my $line = <$file_fh>) {
    chomp($line);
    $line =~ s/["']//g;
    next if $line =~ /^\s*$/;  # skip empty lines

    my @Field_Values = split(",", $line);

    for (my $i = 0; $i < $Field_Names_Count; $i++) {
        $Field_Values[$i] //= "0";

        my $val = $Field_Values[$i];

        # Default to varchar if letters are present
        if ($val =~ /[a-zA-Z]/) {
            $type[$i] = "varchar";
            $length[$i] = length($val) if !defined($length[$i]) || $length[$i] < length($val);
        }
        # Decimal
        elsif ($val =~ /^[+-]?\d+\.\d+$/) {
            $type[$i] = "decimal";
            my ($int_part, $dec_part) = split(/\./, $val);
            $decimal_length1[$i] = length($int_part) if !defined($decimal_length1[$i]) || $decimal_length1[$i] < length($int_part);
            $decimal_length2[$i] = length($dec_part) if !defined($decimal_length2[$i]) || $decimal_length2[$i] < length($dec_part);
        }
        # Integer
        elsif ($val =~ /^-?\d+$/) {
            $type[$i] = "int" if !defined($type[$i]) || $type[$i] ne "decimal";
            $length[$i] = length($val) if !defined($length[$i]) || $length[$i] < length($val);
        }
        # Default fallback
        else {
            $type[$i] = "varchar";
            $length[$i] = length($val) if !defined($length[$i]) || $length[$i] < length($val);
        }
    }

    # Write to insert file
    my $columns_str = join(", ", @Field_Names);
    my @escaped_values = map { s/'/\\'/g; "'$_'" } @Field_Values;
    my $values_str = join(", ", @escaped_values);

    print $values_fh "INSERT INTO $TABLE_NAME ($columns_str) VALUES ($values_str);\n";
    $row_count++;
}

# Write schema
print $table_fh "CREATE TABLE $TABLE_NAME (\n";

for my $i (0 .. $#Field_Names) {
    my $name = $Field_Names[$i];
    print $table_fh "  `$name` ";

    if ($type[$i] eq "decimal") {
        my $p = ($decimal_length1[$i] // 5) + ($decimal_length2[$i] // 2);
        my $s = $decimal_length2[$i] // 2;
        print $table_fh "DECIMAL($p,$s)";
    } elsif ($type[$i] eq "int") {
        my $l = $length[$i] // 10;
        print $table_fh "INT($l)";
    } else {
        my $l = $length[$i] // 50;
        print $table_fh "VARCHAR($l)";
    }

    print $table_fh "," if $i < $#Field_Names;
    print $table_fh "\n";
}

print $table_fh ") ENGINE=$DATABASE_ENGINE DEFAULT CHARSET=$DEFAULT_CHARSET;\n";

# Close all files
close($file_fh);
close($table_fh);
close($values_fh);

print "✔ Processed $Field_Names_Count columns and $row_count rows.\n";
print "✔ Created 'mysqlCreateSchema.sql' and 'mysqlInsertValues.sql'.\n";
