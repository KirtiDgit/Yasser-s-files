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
my $DEBUG = 1;

# @arry = grep {$_} @arry;

sub getDBStorageSize {
    my ($o, $f, $l, $t) = @_;
    my $r;
    
    if(     $l eq 'Read' ) {
        # small # $r = $f==50 ? 17.4 : $f==250 ? 66.1 : $f==1000 ? 259.7 : 'X';
        $r = $f==50 ? 1400 : $f==250 ? 6700 : $f==1000 ? 27500 : 'X';
    } elsif( $l eq 'Read to Encrypted' ) {
        # $r = $f==50 ? 17.4 : $f==250 ? 66.2 : $f==1000 ? 260.4 : 'X';       
        $r = $f==50 ? 1400 : $f==250 ? 6700 : $f==1000 ? 27500 : 'X';
    } elsif( $l eq 'Write' ) {
        if(  $f==50   ){ $r = $t==1 ? 73.8 : $t==5 ? 80.2 : $t==10 ? 93.1 : 'X'; }
        if(  $f==250  ){ $r = $t==1 ? 372.4 : $t==5 ? 404.7 : $t==10 ? 469.4 : 'X'; }
        if(  $f==1000 ){ $r = $t==1 ? 1300 : $t==5 ? 1400 : $t==10 ? 1700 : 'X'; }
    } elsif( $l eq 'Write to Encrypted' ) {
        if(  $f==50   ){ $r = $t==1 ? 73.8 : $t==5 ? 80.2 : $t==10 ? 93.1 : 'X'; }
        if(  $f==250  ){ $r = $t==1 ? 372.4 : $t==5 ? 404.7 : $t==10 ? 469.4 : 'X'; }
        if(  $f==1000 ){ $r = $t==1 ? 1300 : $t==5 ? 1400 : $t==10 ? 1700 : 'X'; }
    }
    
    return $r;
}
sub getFileSystemSize {
    my ($o, $f, $l, $t) = @_;
    my $r;
    
    if(     $l eq 'Read' ) {
        #$r = $f==50 ? 385 : $f==250 ? 442 : $f==1000 ? 636 : 'X';
        $r = $f==50 ? 5500 : $f==250 ? 5500 : $f==1000 ? 5500 : 'X';        
    } elsif( $l eq 'Read to Encrypted' ) {
        #$r = $f==50 ? 432 : $f==250 ? 458 : $f==1000 ? 653 : 'X';  
        $r = $f==50 ? 5600 : $f==250 ? 5600 : $f==1000 ? 5600 : 'X'; 
    } elsif( $l eq 'Write' ) {
        if(  $f==50   ){ $r = $t==1 ? 400 : $t==5 ? 401 : $t==10 ? 402 : 'X'; }
        if(  $f==250  ){ $r = $t==1 ? 450 : $t==5 ? 453 : $t==10 ? 458 : 'X'; }
        if(  $f==1000 ){ $r = $t==1 ? 569 : $t==5 ? 589 : $t==10 ? 628 : 'X'; }
    } elsif( $l eq 'Write to Encrypted' ) {
        if(  $f==50   ){ $r = $t==1 ? 415 : $t==5 ? 416 : $t==10 ? 417 : 'X'; }
        if(  $f==250  ){ $r = $t==1 ? 465 : $t==5 ? 468 : $t==10 ? 473 : 'X'; }
        if(  $f==1000 ){ $r = $t==1 ? 667 : $t==5 ? 686 : $t==10 ? 726 : 'X'; }
    }
    
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
        print sprintf("%s> %s\n", ($enc =~ 'AtRest' ? 'Full DB' : $enc), $o);
        for my $f (qw(1000 250 50)) {  
            print "Document Size: $f-Fields\n";
            my @labels;
            if(     $enc eq 'AtRest' && $o eq 'Insert'){ @labels = ('Write to Encrypted','Write'); }
            elsif(  $enc eq 'AtRest'){ @labels = ('Read to Encrypted','Read'); }             
            for my $l (@labels) {
                print "$l";
                for my $t (qw(1 5 10)) {
next if $o eq 'ReadMultiple' && $t > 1;
#next if $o eq 'Insert' && $f > 50; 
next if $t > 5; 
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
        if(     $enc eq 'AtRest' && $o eq 'Insert'){ @labels = ('Write to Encrypted','Write'); }
        elsif(  $enc eq 'AtRest'){ @labels = ('Read to Encrypted','Read'); }    
        for my $l (@labels) { 
            for my $f (qw(1000 250 50)) {  
                for my $t (qw(1 5 10)) {
next if $o eq 'ReadMultiple' && $t > 1;   
#next if $o eq 'Insert' && $f > 50; 
next if $t > 5; 
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
                        $o eq 'ReadMultiple' ? 'Query' : $o,
                        ($o =~ /^Insert/ ? ($t==1 ? 52000 : $t==5 ? 57000 : $t==10 ? 67000 : 0) : 50000),
                        ($o =~ /^Insert/ ? 'Off' : 'On'),                        
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

printFullCSV('AtRest', qw(Read Insert));   # ReadMultiple 
#printFullCSV('CSFLE', ('Read-E0','Read-E25','Read-E50'));

print("\n");

#printByThread('CSFLE', ('Read-E50','Read-E25','Read-E0'));
printByThread('AtRest', qw(Read Insert))   # ReadMultiple 