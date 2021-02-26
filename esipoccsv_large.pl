#!/usr/bin/perl 
  
use strict; 
use warnings; 
use List::Util qw( min max );

# sudo cpan install JSON
# sudo cpan install Statistics::Basic
use JSON;
use Statistics::Basic qw(:all);


my $PoCDir = '/Users/yasser.elzein/pl/poc';   
my %Data;
my $DEBUG = 0;

# @arry = grep {$_} @arry;

sub getDBStorageSize {
    my ($o, $f, $l, $t) = @_;
    my $r;
    
    if( $l =~ /Encrypted$/ )  { $r = $f==50 ? 1.4 : $f==250 ? 6.7 : $f==1000 ? 27.5 : 'X'; }
    else                      { $r = $f==50 ? 1.4 : $f==250 ? 6.7 : $f==1000 ? 27.5 : 'X'; }
    
    return $r;
}
sub getFileSystemSize {
    my ($o, $f, $l, $t) = @_;
    my $r;
    
    if( $l =~ /Encrypted$/ )  { $r = $f==50 ? 6.5 : $f==250 ? 6.5 : $f==1000 ? 6.5 : 'X'; }
    else                      { $r = $f==50 ? 6.1 : $f==250 ? 6.1 : $f==1000 ? 6.1 : 'X'; }
    
    return $r;
}

sub processTrimmed {
    my ($dir) = @_;
    
    my ($encMethod, $op, $f, undef) = $dir =~ /\/([A-z]+)\/([A-z]+)F(\d+)$/;
    my @files = glob( $dir . '/Trimmed/*' );              # files in Trimmed directory
    foreach my $filename (@files) {
        my %h;
        open(FH, '<', $filename) or die $!;
        while(<FH>){
            next if $. < 2;                              # skip headers line
            my @fields = split "," , $_;  
            next if $fields[3] ne "200";                 # skip if responseCode is not 200 
            next if $fields[2] =~ /(Connect to DB)+/;    # skip db connection threads
    
            push @{$h{ $fields[2] }}, $fields[1];        # push 'elapsed' into hash of 'label' array
        }
        close(FH);

        print "$filename\n";
        my ($t) = $filename =~ /T(\d+)_Trimmed\.csv$/;
        #print "OPERATION: $op, FIELDS: $f, THREADS: $t\n";
        for my $label (sort keys %h) {
            $Data{$encMethod}{$op}{$f}{$t}{$label}{count}   = scalar( @{$h{$label}} );            
            $Data{$encMethod}{$op}{$f}{$t}{$label}{median} = median( @{$h{$label}} );
            $Data{$encMethod}{$op}{$f}{$t}{$label}{mean} = mean( @{$h{$label}} );
            $Data{$encMethod}{$op}{$f}{$t}{$label}{stddev}  = stddev( @{$h{$label}} );
            $Data{$encMethod}{$op}{$f}{$t}{$label}{min}  = min( @{$h{$label}} );
            $Data{$encMethod}{$op}{$f}{$t}{$label}{max}  = max( @{$h{$label}} );
            $Data{$encMethod}{$op}{$f}{$t}{$label}{statistics} = statistics($dir, $filename =~ /\/([A-z0-9]+)_Trimmed\.csv$/, $label);
            $Data{$encMethod}{$op}{$f}{$t}{$label}{resources} = resources($dir, $filename =~ /\/([A-z0-9]+)_Trimmed\.csv$/, $label);
        }
    } 
}

sub resources {
    my ($dir, $statsFolder, $label) = @_;
    $label =~ tr/ //ds;
    
    my %h;
    my $filename = $dir .'/'. $statsFolder .'/Metrics/'. $label .'.csv';
    #print $filename."\n";
    open(FH, '<', $filename) or die $!;
    while(<FH>){
        next if $. < 2;                              # skip headers line
        my @fields = split "," , $_;  
    
        push @{$h{ $fields[2] }}, $fields[4];        # push 'elapsed' into hash of 'label' array
    }
    close(FH);
    
    my %r;
    for my $k (sort keys %h) {
        $r{$k} = mean( @{$h{$k}} );
    }
    
    return \%r;
}

sub statistics {
    my ($dir, $statsFolder, $label) = @_;

    my $json;
    {
        local $/;   #Enable 'slurp' mode
        open my $fh, "<", "$dir/".$statsFolder.'/statistics.json';
        $json = <$fh>;
        close $fh;
    }
  
    my $data = decode_json($json);
    return $data->{$label};
}

sub printByThread {
    my ($enc, @ops) = @_; 
    
    print "\n";    
    for my $o (@ops) { 
        my @labels;
        if(     $enc eq 'AtRest' && $o =~ /^Insert/){ @labels = ('Write to Encrypted','Write'); }
        elsif(  $enc eq 'AtRest'){ @labels = ('Read to Encrypted','Read'); } 
    
        print sprintf("%s> %s\n", ($enc =~ 'AtRest' ? 'Full DB' : $enc), $o);
        for my $f (qw(1000 250 50)) {  
            print "Document Size: $f-Fields\n";            
            for my $l (@labels) {
                print "$l";
                my @threads = (1, 5);
                @threads = (1) if $o =~ /^Insert$/ || $o eq 'ReadMultiple' || $o eq 'ReadNoIndex';                
                for my $t (@threads) {
                    print sprintf(", %.2f",
                        $Data{$enc}{$o}{$f}{$t}{$l}{mean}
                    );
                }
                print "\n";
            }
        }
    }
}

sub printFullCSV {
    my ($enc, @ops) = @_;
        
    print "\n";
    for my $o (@ops) {  
        my @labels;
        if(     $enc eq 'AtRest' && $o =~ /^Insert/){ @labels = ('Write to Encrypted','Write'); }
        elsif(  $enc eq 'AtRest'){ @labels = ('Read to Encrypted','Read'); } 
        
        for my $l (@labels) { 
            for my $f (qw(1000 250 50)) {  
                my @threads = (1, 5);
                @threads = (1) if $o =~ /^Insert$/ || $o eq 'ReadMultiple' || $o eq 'ReadNoIndex';
                for my $t (@threads) {
                    print ">>op:$o label:$l f:$f t:$t\n" if $DEBUG;  
                    if ($DEBUG) {
                        print $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{sampleCount}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{errorCount}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{count}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{median}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{mean}."\n"; 
                        print $Data{$enc}{$o}{$f}{$t}{$l}{stddev}."\n"; 
                        print $Data{$enc}{$o}{$f}{$t}{$l}{min}."\n"; 
                        print $Data{$enc}{$o}{$f}{$t}{$l}{max}."\n"; 
                        print $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{pct2ResTime}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{pct3ResTime}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{throughput}."\n";
                        print getDBStorageSize($o, $f, $l, $t)."\n";
                        print getFileSystemSize($o, $f, $l, $t)."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{ClientCPU}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{ClientMEM}."\n";                   
                        print $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{DBCPU}."\n";
                        print $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{DBMEM}."\n";
                    }
                    print sprintf(",,,%s,%s, %s, $f, %s, $t, %s, Local, %s, %s, %s, %.2f, %.2f, %.2f, %.2f, %.2f, %s, %s, %.2f,%.1f,%s,%.2f,%.2f,%.2f,%.2f\n",  
                        ($enc =~ 'AtRest' ? 'Full DB' : $enc),
                        ($enc =~ 'AtRest' ? ($l =~ /Encrypted$/ ? 'All' : 'None') : ($l =~ /\-E(\d+)$/)),
                        $o eq 'ReadMultiple' ? 'Query' : $o =~ /^Insert/ ? 'Insert' : $o =~ /^Read/ ? 'Read' : $o,
                        ($o =~ /^Insert/ ? ($t==1 ? 1002000 : $t==5 ? 1012000: 'X') : 1000000),
                        ($o =~ /^Insert$/ || $o =~ /NoIndex$/ ? 'Off' : 'On'),                        
                        $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{sampleCount},
                        $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{errorCount},
                        $Data{$enc}{$o}{$f}{$t}{$l}{count},
                        $Data{$enc}{$o}{$f}{$t}{$l}{median}, 
                        $Data{$enc}{$o}{$f}{$t}{$l}{mean}, 
                        $Data{$enc}{$o}{$f}{$t}{$l}{stddev}, 
                        $Data{$enc}{$o}{$f}{$t}{$l}{min}, 
                        $Data{$enc}{$o}{$f}{$t}{$l}{max}, 
                        $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{pct2ResTime},
                        $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{pct3ResTime},
                        $Data{$enc}{$o}{$f}{$t}{$l}{statistics}->{throughput},
                        getDBStorageSize($o, $f, $l, $t),
                        getFileSystemSize($o, $f, $l, $t),
                        $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{ClientCPU},
                        $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{ClientMEM},                        
                        $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{DBCPU},
                        $Data{$enc}{$o}{$f}{$t}{$l}{resources}->{DBMEM}
                    );
                }
            }
        }
    }
    print "\n";
}


#
# main
# 
foreach my $encMethodDir ( glob( $PoCDir . '/*' ) ) {        # encMethodDir: /poc/AtRest
    foreach my $opDir ( glob( $encMethodDir . '/*' ) ) {
        next if $opDir =~ /\.zip$/;
        processTrimmed($opDir);
    }
}

printFullCSV('AtRest', qw(Read ReadNoIndex Insert InsertIndex));   # ReadMultiple 
#printFullCSV('CSFLE', ('Read-E0','Read-E25','Read-E50'));

print("\n");

#printByThread('CSFLE', ('Read-E50','Read-E25','Read-E0'));
printByThread('AtRest', qw(Read ReadNoIndex Insert InsertIndex))   # ReadMultiple 