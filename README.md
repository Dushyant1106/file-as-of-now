# file-as-of-now


#!/usr/bin/perl
use File::Copy;

$TABLE_NAME = "nasdaq_prices";
$DATABASE_ENGINE = "InnoDB";
$DEFAULT_CHARSET = "latin1";


$filename = "prices.csv";


open(TABLE, ">mysqlCreateSchema.sql") || die "Failed to redirect output";
open(VALUES, ">mysqlInsertValues.sql") || die "Failed to redirect output";


$count = 0;


$Columns_Values = "";


open FILE, "$filename" or die $!;


my $columns = <FILE>;


chop $columns;

chop $columns;

$columns =~ s/'/\\'/g;


$columns =~ s/\"//;

chop $columns;


$columns =~ s/ /_/g;



@Field_Names = split(",",$columns);


$Field_Names_Count = $#Field_Names;

$Field_Names_Count_Plus_One = $Field_Names_Count + 1;


$field_count = 0;


if ($count == 0)

{

$column_count = 0;

   while ($column_count <= $Field_Names_Count)
   
   {
      if ($column_count < $Field_Names_Count)
   
      {
         $Columns_Values = $Columns_Values . $Field_Names[$column_count] . ", ";
      }
      
      
      if ($column_count == $Field_Names_Count)
   
      {
         $Columns_Values = $Columns_Values . $Field_Names[$column_count];
      }

      $column_count++;
   }
   

}

$count = 0;


while (<FILE>)

{


chomp $_;


$_ =~ s/\"//;


chop $_;


@Field_Values = split(",",$_);

while ($field_count <= $Field_Names_Count )

{


   $Field_Values[$field_count] =~ s/'/\\'/g;


         if (length($Field_Values[$field_count]) < 1)
         
         {
            $Field_Values[$field_count] = "0";
         }


         if ( $Field_Values[$field_count] =~ m/[a-zA-Z]/)
         
         {
               $type[$field_count] = "varchar";
               

               if ($length[$field_count] < 'length($Field_Values[$field_count])')
            
               {
                  $length[$field_count] = length($Field_Values[$field_count]);
               }
         }
   
   if ($type[$field_count] ne "varchar")
   
   {
         if ( $Field_Values[$field_count] =~ m/[^a-zA-Z]/)
   
         {
            if ($type[$field_count] ne "decimal")
            
            {
               $type[$field_count] = "int";
               
               if ($length[$field_count] lt 'length($Field_Values[$field_count])')
               {
                  $length[$field_count] = length($Field_Values[$field_count]);
               }
            }
         }
   
         if ( $Field_Values[$field_count] =~ m/[0-9.]/)
   
         {
               @count_periods = split("\\.",$Field_Values[$field_count]);
               $number_of_periods = $#count_periods;
            
            
            if ($number_of_periods > 1)
            
            {
   
            $type[$field_count] = "varchar";
            
         
               if ($length[$field_count] < 'length($Field_Values[$field_count])')
               {
                  $length[$field_count] = length($Field_Values[$field_count]);
               }
   
   
                  $decimal_length1[$field_count] = "";
                  $decimal_length2[$field_count] = "";
               }
   
            if ($number_of_periods == 1)
            
            {
               $type[$field_count] = "decimal";
               @split_decimal_number = split("\\.",$Field_Values[$field_count]);
               
               if ($decimal_length1[$field_count] lt length($split_decimal_number[0]))
               
               {
                  $decimal_length1[$field_count] = length($split_decimal_number[0]);
               }
               
               if ($decimal_length2[$field_count] lt length($split_decimal_number[1]))
               
               {
                  $decimal_length2[$field_count] = length($split_decimal_number[1]);
               }
                           
            }
   
         }
                  
         if ( $Field_Values[$field_count] =~ m/[^0-9.]/)
         
         {
               $type[$field_count] = "varchar";
   
               if ($length[$field_count] lt 'length($Field_Values[$field_count])')
            
               {
                  $length[$field_count] = length($Field_Values[$field_count]);
               }
   
         }
   
   }
   
   else
   
   {         
   
               if ($length[$field_count] < length($Field_Values[$field_count]))
            
               {
                  $length[$field_count] = length($Field_Values[$field_count]);
               }
   
   
   }
   
   
   
         if (length($Field_Values[$field_count]) < 1)
         
         {
            $Field_Values[$field_count] = "";
         }

   
      if ($field_count == 0)
      
      {
         print VALUES "insert into $TABLE_NAME ($Columns_Values) \nvalues ('$Field_Values[$field_count]'";
      }
      
         if ($field_count > 0 && $field_count < $Field_Names_Count_Plus_One)
         
         {
            print VALUES ", '$Field_Values[$field_count]'";
         }
         
      $field_count++;
      }
   
         if ($field_count == $Field_Names_Count_Plus_One)
         
         {
            $field_count = 0;
            $count++;
            print VALUES ");\n";
         }
   


}

print TABLE "\n\nCREATE TABLE `$TABLE_NAME` (\n";

$count_columns = 0;


while ($count_columns < $Field_Names_Count_Plus_One)

{
   if (length($Field_Names[$count_columns]) > 0)
   
   {
      if ($type[$count_columns] =~ "decimal")
      
      {
         $decimal_field_length = $decimal_length1[$count_columns] + $decimal_length2[$count_columns];
         print TABLE " `$Field_Names[$count_columns]` $type[$count_columns] ($decimal_field_length,$decimal_length2[$count_columns])";
      }
      
      else
      
      {
         print TABLE " `$Field_Names[$count_columns]` $type[$count_columns] ($length[$count_columns])";
      }
   
      if ($count_columns < $Field_Names_Count)
      
      {
         print TABLE ",\n";
      }
      
      if ($count_columns == $Field_Names_Count_Plus_One)
      
      {
         print TABLE "\n\n";
      }
      
   }

$count_columns++;

}

print "Processed $column_count columns and $count lines.\n";

print TABLE "\n) ENGINE=$DATABASE_ENGINE DEFAULT CHARSET=$DEFAULT_CHARSET\n";

print TABLE "\n\n";

close(FILE);

exit;


print "Process completed.\n";
