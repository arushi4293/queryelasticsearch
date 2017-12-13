#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Getopt::Long;
use LWP::UserAgent;
use JSON;
use DBM::Deep;
use Log::Log4perl qw(get_logger :levels);
use Parallel::ForkManager;

my $eas_hash_path = "/var/one.com-eas-dashboard/eas_hash";
my $eas_log_path = "/var/one.com-eas-dashboard/eas.log";
my $eas_timestamp_path = "/var/one.com-eas-dashboard/eas.timestamp";
my $logelastic_url = "http://logelastic5.public.one.com:9200";

my %eas_commands = (
	'executed-commands' => 'host: eas* AND facility: user AND (message: \"Executing command\" OR message: \"throttle\")',
	'requested-commands' => 'host: eas* AND facility: local0 AND message: \"POST\" and message: User',
);


my %eas_errors = (
	"uncaught-exception" => 'host: eas* AND message: \"Uncaught Exception\"',
	"sendmail-error" => 'host: eas* AND message: \"Could not send mail\"',
	"database-connection-error" => 'host: eas* AND message: \"Could not connect to database\"',
	"folder-create-error" => 'host: eas* AND message: \"Error creating folder\"',
	"invalid-sync-key-errror" => 'host: eas* AND message: \"InvalidSyncKey\"',
	"stderr" => 'host: eas* AND message: \"STDERR\" AND NOT message:  \"Uncaught Exception\"',
	"fatal-error" => 'host: eas* AND message: \"FATAL ERROR\"',
	"auth-errors" => "host: eas* AND message: AUTHENTICATIONFAILED",
);

my %eas_executed_commands = (
	"Ping" => 'host: eas* AND facility: user AND message: \"Executing command Ping\"', 
	"Sync" => 'host: eas* AND facility: user AND message: \"Executing command Sync\"',
	"FolderSync" => 'host: eas* AND facility: user AND message: \"Executing command FolderSync\"',
	"Search" => 'host: eas* AND facility: user AND message: \"Executing command Search\"',
	"ItemOperations" => 'host: eas* AND facility: user AND message: \"Executing command ItemOperations\"', 
	"Settings" => 'host: eas* AND facility: user AND message: \"Executing command Settings\"',
	"GetItemEstimate" => 'host: eas* AND facility: user AND message: \"Executing command GetItemEstimate\"',
	"MoveItems" => 'host: eas* AND facility: user AND message: \"Executing command MoveItems\"',
	"GetAttachment" => 'host: eas* AND facility: user AND message: \"Executing command GetAttachment\"',
	"SmartForward" => 'host: eas* AND facility: user AND message: \"Executing command SmartForward\"',
	"SendMail" => 'host: eas* AND facility: user AND message: \"Executing command SendMail\"',
	"FolderCreate" => 'host: eas* AND facility: user AND message: \"Executing command  FolderCreate\"',
	"Provision" => 'host: eas* AND facility: user AND message: \"Executing command Provision\"',
	"FolderUpdate" => 'host: eas* AND facility: user AND message: \"Executing command FolderUpdate\"',
	"MeetingResponse" => 'host: eas* AND facility: user AND message: \"Executing command MeetingResponse\"'
);


my %error_types = (
	"eas-commands" => \%eas_commands, 
        "eas-errors" => \%eas_errors,
        "eas-executed-commands" => \%eas_executed_commands,
);

my %index_types = (
	"eas-commands" => "logstash",
	"eas-executed-commands" => "logstash",
	"eas-errors" => "logstash",
);


my $previous_timestamp;
my $current_timestamp;
my $miliseconds=0;
my $indextype = "logstash";
my $date = `date +%Y.%m.%d`;
chomp($date);
my $logger = setuplogging("EAS");

my $easdb = DBM::Deep->new("$eas_hash_path/eas.db");
my $requested = DBM::Deep->new(
	file => "$eas_hash_path/requested.db",
	type => DBM::Deep->TYPE_ARRAY
	);
my $executed = DBM::Deep->new(
	file => "$eas_hash_path/executed.db",
	type => DBM::Deep->TYPE_ARRAY
	);
my $ping = DBM::Deep->new(
	file => "$eas_hash_path/ping.db",
	type => DBM::Deep->TYPE_ARRAY
	);
my $sync = DBM::Deep->new(
	file => "$eas_hash_path/sync.db",
	type => DBM::Deep->TYPE_ARRAY
	);
my $requested_position = DBM::Deep->new("$eas_hash_path/requested_position.db");
my $executed_position = DBM::Deep->new("$eas_hash_path/executed_position.db");
my $sync_position = DBM::Deep->new("$eas_hash_path/sync_position.db");
my $ping_position = DBM::Deep->new("$eas_hash_path/ping_position.db");

if( $#ARGV == -1 ){
	print "Filters result from current date ($date)\n";
	print "Usage: Use ANY ONE of compulsory options --query OR --querytype OR --generate-hash\nperl query_elasticsearch.pl --query <QUERY>\nOR\nperl query_elasticsearch.pl --querytype <QUERY_TYPE>\nOR\nperl query_elasticsearch.pl --generate-hash\n";
	print "========================================================================\n";
	print "THe following options are available:\n";
	print "1) --time <TIME> => Filter results for mentioned time measured from the current time[10m|3h|50s], Eg: 10m (from past 10mins)\n";
	print "2) --pod <POD> => Filter results from a particular pod [cph3/wdc/legacy]\n";
	print "3) --date <DATE> => Filter results from a particular date [YYYY.mm.dd format]\n";
	print "5) --all => Fetch results from all available dates ( for logstash- 3 days and for webdev-30days)\n";
	print "6) --summarize => To summarize results (Eg: give per user count for eas related errors)\n";
	print "7) --query <QUERY> => To filter results based on a query from logstash index (use --webdev with --query <QUERY> option for querying on webdev index)\n";
	print "8) --querytype <QUERYTYPE> => To filter results based on defined querytype classes \n";
	print "\n<QUERY_TYPE>\n";
	foreach my $key (keys%error_types){
		print "* $key\n";
	}	
	print "========================================================================\n";
	exit;
}

my $errorqueryhash;
my ($time,$errortype,$pod,$errorquery,$all,$summarize,$hash,$html);
GetOptions ("time=s" => \$time,    # numeric
            "querytype=s"   => \$errortype,      # string
            "pod=s"   => \$pod,      # string
            "date=s"   => \$date,      # string
            "query=s"   => \$errorquery,      # string
            "all"  => \$all,
	    "summarize" => \$summarize,
	    "generate-hash" => \$hash,
	    "html" => \$html
) 
or warn("Error in command line arguments\n");



if( (! defined $errorquery && ! defined $errortype && ! defined $hash) || (defined $errorquery && defined $errortype && defined $hash)){
	print "Usage: Use ANY ONE of compulsory options --query OR --querytype OR --generate-hash\nperl query_elasticsearch.pl --query <QUERY>\nOR\nperl query_elasticsearch.pl --querytype <QUERY_TYPE>\nOR\nperl query_elasticsearch.pl --generate-hash\n";
	print "\n<QUERY_TYPE>\n";
	print "========================================================================\n";
	foreach my $key (keys%error_types){
		print "* $key\n";
	}	
	print "========================================================================\n";
	exit;
} 
if ( defined $errortype &&  ! defined $error_types{$errortype} ){	
	print "Error!!! Please choose a valid QUERY_TYPE\nUsage: perl query_elasticsearch.pl --errortype <QUERY_TYPE>\n";
	print "\n<QUERY_TYPE>\n";
	foreach my $key (keys%error_types){
		print "* $key\n";
	}	
	print "\n";
	exit;
}

if(defined $date && $date !~ m/(\d{4})\.(\d{2})\.(\d{2})/){
	print "Error!! Please specify date in the following format: YYYY.mm.dd\n";
	exit;
}


#Index type from which information needs to be fetched webdev/logstash
if(defined $errortype){
	$indextype = $index_types{$errortype};
	$errorqueryhash = $error_types{$errortype};
}


if(defined $pod){
	if($pod eq "legacy"){
		$pod="public";
	}
	if($indextype eq "logstash"){
		$errorquery = "$errorquery AND host: ${pod}*";	
	}
	else{
		$errorquery = "$errorquery AND pod: ${pod}*";	
	}
}

if(defined $errorquery){
	$errorqueryhash->{'custom_query'} = $errorquery;
}



if(defined $all){
	my @indices_list = find_all_indices($indextype);
	foreach my $index ( @indices_list){
       		my $query_url = "$logelastic_url/$index/_count?pretty";
		print "============================\n";
		print "$index: \n";
		print "============================\n";
		foreach my $errorqueryname ( keys%{$errorqueryhash}){
			my $errorquery = $errorqueryhash->{$errorqueryname};
			my $query_data = form_query_data($time,$errorquery,$current_timestamp,$previous_timestamp);
			my $count = get_count($query_url,$query_data);
			print "$errorqueryname: $count \n";
		}
	}
	exit;
}

if(defined $hash){
	$current_timestamp = `date +%s%N | cut -b1-13`; 
	if( -f "$eas_timestamp_path"){
		$previous_timestamp = `cat $eas_timestamp_path`;
		$logger->info("Last time of successfull execution was $previous_timestamp");
	}
	my $time_range = (($current_timestamp - $previous_timestamp)/(1000*60));
	$logger->info("Querying data for $time_range minutes");
	$errorqueryhash = \%eas_commands;
}

if(! defined $hash){
	if(defined $html){
	if($errortype eq "eas-errors"){
		print "<div class='left'><h3>EAS ERRORS COUNT</h3>";
		print '<table class="onecom__table"><thead><tr><th>Error</th><th>Count</th></thead><tbody>'; 
	}
	elsif($errortype eq "eas-executed-commands"){
		print "<div class='left'><h3>EAS COMMANDS COUNT</h3>";
		print '<table class="onecom__table"><thead><tr><th>Commands</th><th>Count</th></thead><tbody>'; 
	}
	}
	run_simultaneous();
	if(defined $html){
		print "</tbody></table></div>";
	}
}

if(defined $hash){
	my $indexname = "$indextype-$date";
        my $query_url = "$logelastic_url/$indexname/_count?pretty";
        foreach my $errorqueryname ( sort keys%{$errorqueryhash}){
                my $errorquery = $errorqueryhash->{$errorqueryname};
                my $query_data = form_query_data($time,$errorquery,$current_timestamp,$previous_timestamp);
                my $count = get_count($query_url,$query_data);
                $logger->info("$errorqueryname : $count");
                generate_hash($indexname,$query_data,$errorqueryname);
        }
        $logger->info("Dumping current timestamp");
        open(my $fh,'>',"$eas_timestamp_path") or die "Cannot open file \n";
        print $fh $current_timestamp
}


#FUNCTIONS


sub run_simultaneous{
	my $query_url = "$logelastic_url/$indextype-$date/_count?pretty";
 	my $manager = new Parallel::ForkManager(20);
  foreach my $errorqueryname ( sort keys%{$errorqueryhash}){
	$manager->start and next;
        my $errorquery = $errorqueryhash->{$errorqueryname};
        my $query_data = form_query_data($time,$errorquery,$current_timestamp,$previous_timestamp);
        my $count = get_count($query_url,$query_data);
        if(defined $html){
		print "<tr><td>$errorqueryname</td><td>$count</td></tr>";
	}
	else{
		print "$errorqueryname : $count\n";
	}
  	$manager->finish(0);
  }
  $manager->wait_all_children;


}




sub generate_hash{
	my $indexname = shift;
	my $query_data = shift;
	my $errortype = shift;
	my $counter = 1;
	#print "I am  initiating sending $counter request for $errortype ";
	my $query_url = "$logelastic_url/$indexname/_search?size=10000&scroll=10m";
        my $response_json = http_get_request($query_url,$query_data);
	my $response = from_json($response_json);
	my $scroll_id = $response->{"_scroll_id"};
	my $list = $response->{"hits"}->{"hits"};
	analyse_batch($list,$errortype);
	my $scroll_size = scalar @{$list};
        $logger->info("Initiating  $counter request for $errortype  and scroll size is $scroll_size");
	#print "and scroll size is $scroll_size \n";
	while($scroll_size > 0){
		$counter++;
		#print "Sending request Number = $counter request and ";
        	$query_url = "$logelastic_url/_search/scroll";
		my $scroll_data = form_scroll_query($scroll_id);
	        $response_json = http_get_request($query_url,$scroll_data);
		$response = from_json($response_json);
		$scroll_id = $response->{"_scroll_id"};
		$list = $response->{"hits"}->{"hits"};
		analyse_batch($list,$errortype);
		$scroll_size = scalar @{$list};
        	$logger->info("Initiating  $counter request for $errortype  and scroll size is $scroll_size");
		#print "Scroll size =$scroll_size \n";
	}

}

sub analyse_batch{
	my $list = shift;
	my $type =shift;
        if($type eq "executed-commands"){
                foreach my $element ( @$list ){
                        my $host = $element->{_source}->{host};
                        my $message = $element->{_source}->{message};
                        my @fields = split(' ',$message);
                        my $domainfield=$fields[3];
                        $domainfield =~ m/\[(.*)\/(.*):/;
                        my $domain = $1;
                        my $deviceid = $2;
			my $command_type = $fields[5];
			if($command_type =~ "throttle"){
	                        $easdb->{$host}->{$domain}->{$deviceid}->{'throttled'}++;
			}
			else{
				my $command = $fields[8];
                        	$easdb->{$host}->{$domain}->{$deviceid}->{'executed'}->{$command}++;
				                       		
				my $current_value = $easdb->{$host}->{$domain}->{$deviceid}->{'executed'}->{$command};

				if($command eq "Ping"){
					my $ping_pos = 0;
					if( defined $ping_position->{$domain} ){
						my $previous_value = $current_value - 1;
						delete $ping->[$previous_value]->[$ping_position->{$domain}];
					}
					if( defined $ping->[$current_value]){	
						$ping_pos = scalar (@{$ping->[$current_value]});
					}
					$ping_position->{$domain} = $ping_pos;
					push(@{$ping->[$current_value]},$domain); 
				}
				elsif($command eq "Sync"){
					my $sync_pos = 0;
					if( defined $sync_position->{$domain} ){
						my $previous_value = $current_value - 1;
						delete $sync->[$previous_value]->[$sync_position->{$domain}];
					}
					if( defined $sync->[$current_value]){	
						$sync_pos = scalar (@{$sync->[$current_value]});
					}
					$sync_position->{$domain} = $sync_pos;
					push(@{$sync->[$current_value]},$domain); 
					
				}
			 	$easdb->{$host}->{$domain}->{'total-executed-command'}++;
				$current_value = $easdb->{$host}->{$domain}->{'total-executed-command'};
				my $domain_position = 0;
				if( defined $executed_position->{$domain} ){
					my $previous_value = $current_value - 1;
					delete $executed->[$previous_value]->[$executed_position->{$domain}];
				}
				if( defined $executed->[$current_value]){	
					$domain_position = scalar (@{$executed->[$current_value]});
				}
				$executed_position->{$domain} = $domain_position;

				push(@{$executed->[$current_value]},$domain); 
				$easdb->{$host}->{'total-executed-command'}++;
                	} 
		}
        }
        elsif($type eq "requested-commands"){
                foreach my $element ( @$list ){
                        my $host = $element->{_source}->{host};
                        my $message = $element->{_source}->{message};
                        my @fields = split(' ',$message);
                        my $request = $fields[14];
                        my $response_code = $fields[16];
                        my ($domain,$command,$devicetype,$deviceid) = "Unknown";
			if($request =~ m/User=(.*?)(&|$)/){
                       	 	my $domain = $1;
			}
		        if($request =~ m/Cmd=(.*?)(&|$)/){
                      		 $command = $1;
              		}
		        if($request =~ m/DeviceType=(.*?)(&|$)/){
                      		 $devicetype = $1;
                        }
			if($request =~ m/DeviceId=(.*?)(&|$)/){
                        	 $deviceid = $1;
			}
                	$easdb->{$host}->{$domain}->{$deviceid}->{'requested'}->{$command}++;
                        $easdb->{$host}->{$domain}->{'total-requested-command'}++;
			$easdb->{$host}->{'total-requested-command'}++;
			my $current_value = $easdb->{$host}->{$domain}->{'total-requested-command'};
			my $domain_position = 0;
			if( defined $requested_position->{$domain} ){
				my $previous_value = $current_value - 1;
				delete $requested->[$previous_value]->[$requested_position->{$domain}];
			}
			if( defined $requested->[$current_value]){	
				$domain_position = scalar (@{$requested->[$current_value]});
			}
			$requested_position->{$domain} = $domain_position;
			push(@{$requested->[$current_value]},$domain); 
			if(! defined  $easdb->{$host}->{$domain}->{'total-executed-command'}){
				$easdb->{$host}->{$domain}->{'total-executed-command'} = 0;
			}
		}
        }
}


sub setuplogging {
        my $category = shift;
	# Here EAS is just a Logger category 
        my $logger = get_logger($category);
	# it defines that all logs above info log level will be logged

	$logger->level($INFO);
	#Appender are needed to write logs to file/screen/sockets instead of STDOUT
 #       my $appender = Log::Log4perl::Appender->new(
  #              "Log::Dispatch::Syslog",
  #              min_level => "debug",
   #             ident => "MeetingApp",
    #            facility => "local5",
     #   );

	my $appender = Log::Log4perl::Appender->new(
		"Log::Log4perl::Appender::File",	
		filename => $eas_log_path,
		mode => "append",
		min_level => "debug",
		ident => "EAS",
	);


	#Layout is used to define the format for logging
        my $layout = Log::Log4perl::Layout::PatternLayout->new(
	#d=> date, p => priority, F{1}=> filename,L =>lineno, M => Methodname, m=> message, n=> newline
                "%d %p> %F{1}:%L %M - %m%n"
        );
        $appender->layout($layout);
        $logger->add_appender($appender);
        return $logger;
}


sub get_count{
	my $query_url = shift;
	my $query_data = shift;
	my $response_json = http_get_request($query_url,$query_data);
	my $response = from_json($response_json);
	my $count = $response->{"count"};
	return $count;
}

sub find_all_indices{
	my $indextype = shift;
	my $url = "$logelastic_url/_aliases?pretty=1";
	my $indices_json = http_get_request($url);
	my $indices = from_json($indices_json);
	my @indices_list;
	foreach my $key (sort keys%{$indices}){
		if($key =~ m/$indextype/){
			push @indices_list,$key;
		}
	}
	return @indices_list;
}

sub http_get_request{
	my $url =shift;
	my $query_data = shift;
	my $ua = LWP::UserAgent->new;
	my $req = HTTP::Request->new(GET => "$url");
	$req->content($query_data);
	my $res = $ua->request($req);
	my $response;
	if($res->is_success) {
		$response = $res->content;
	} 
	else{
		my $err = $res->status_line;
		print Dumper($res);
		print "Error in fetching query from logelastic server $url: $err\n";
		exit;
	}
	return $response;
}


sub form_scroll_query{
	my $scroll_id = shift;
	my $query_data =  <<"END_MSG";
{
    "scroll" : "10m", 
    "scroll_id" : "$scroll_id" 
}
END_MSG
}


sub form_query_data{
	my $time = shift;
	my $query = shift;
	my $current_ts = shift;
	my $previous_ts =shift;

	my $query_data;
if( ! defined $time ){
	if(defined $current_ts && defined $previous_ts){
        	$query_data = <<"END_MSG";
{       
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "$query",
          "analyze_wildcard": true
        } 
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "\@timestamp": {
                  "gt": $previous_ts,
                  "lte": $current_ts,
                  "format": "epoch_millis"
                } 
              } 
            } 
          ], 
          "must_not": []
        }         
      }  
    }  
  }  
}  
END_MSG
	}     
	elsif(defined $current_ts){
		$query_data = <<"END_MSG";
{
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "$query",
          "analyze_wildcard": true
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "\@timestamp": {
                  "lte": $current_ts,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  }
}
END_MSG
        }	
	else{
		$query_data = <<"END_MSG";
{
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "$query",
          "analyze_wildcard": true
        }
      } 
   }
 }
}
END_MSG
	}
}

else{
	my $miliseconds = findmiliseconds($time);
	my $current = `date +%s%N | cut -b1-13`;
	my $previous = ($current-$miliseconds);
	$query_data = <<"END_MSG";
{
  "query": {
    "filtered": {
      "query": {
        "query_string": {
          "query": "$query",
          "analyze_wildcard": true
        }
      },
      "filter": {
        "bool": {
          "must": [
            {
              "range": {
                "\@timestamp": {
                  "gte": $previous,
                  "lte": $current,
                  "format": "epoch_millis"
                }
              }
            }
          ],
          "must_not": []
        }
      }
    }
  }
}
END_MSG
}
return $query_data;
}

sub findmiliseconds{
	my $time = shift;
	my $milisecond;
	if($time =~ m/(\d+)([m|s|h])/){
		my $digit = $1;
		my $letter = $2;
		if($letter eq "s"){
			$milisecond = $digit*1000;	
		}
		elsif($letter eq "m"){
			$milisecond = $digit*1000*60;	
		}
		elsif($letter eq "h"){
			$milisecond = $digit*1000*60*60;	
		}
		
	}
	else{
		print "Please specify a valid time format in s/m/h: eg- 10m/30s/5h\n";
		exit;
	}
	return $milisecond;
}



