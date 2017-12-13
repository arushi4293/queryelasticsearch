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
use DateTime;

#Creating a new hash directory per day with name as eas_hash_<DATE> in YYYY.mm.dd format
my $date = `date +%Y.%m.%d`;
chomp($date);
my $eas_hash_path = "/var/one.com-eas-dashboard/eas_hash_${date}";
if( ! -d  $eas_hash_path){
	mkdir $eas_hash_path;
}

#Create  new snapshot directory for previous day with name as <DATE> in YYYY-mm-dd format	
my $yesterday_date = `date -d "yesterday" '+%Y-%m-%d'`;
chomp($yesterday_date);
my $snapshot_path = "/var/www/eas-dashboard/snapshots/$yesterday_date";
if(! -d $snapshot_path){
	mkdir $snapshot_path;
	create_html_dumps($snapshot_path,$yesterday_date);	 
}

#Declaring glocabl vaiables and hashes
my $eas_log_path = "/var/one.com-eas-dashboard/eas.log";
my $eas_timestamp_path = "/var/one.com-eas-dashboard/eas.timestamp";
my $logelastic_url = "https://kibana.one.com:9200";
my $indextype = "logstash";

#Hash containing query string for local0 logs , user logs of eas and mailstorge logs
my %eas_commands = (
	'executed-commands' => 'host: eas* AND facility: user AND (message: \"Executing command\" OR message: \"throttle\")',
	'requested-commands' => 'host: eas* AND facility: local0 AND message: \"Microsoft-Server-ActiveSync\" and message: User',
	'imap-hits' => 'host:(mailstorage* OR  (/mail[0-9].*/ AND local.one.com)) AND message: \"rip=46.30.211.97\" AND ident: dovecot AND message: \"imap-login\"',
);

#Hash containing query string for all eas-errors
my %eas_errors = (
	"uncaught-exception" => 'host: eas* AND message: \"Uncaught Exception\"',
	"sendmail-error" => 'host: eas* AND message: \"Could not send mail\"',
	"database-connection-error" => 'host: eas* AND message: \"Could not connect to database\"',
	"folder-create-error" => 'host: eas* AND message: \"Error creating folder\"',
	"invalid-sync-key-error" => 'host: eas* AND message: \"InvalidSyncKey\"',
	"stderr" => 'host: eas* AND message: \"STDERR\" AND NOT message:  \"Uncaught Exception\"',
	"fatal-error" => 'host: eas* AND message: \"FATAL ERROR\"',
	"auth-errors" => "host: eas* AND message: AUTHENTICATIONFAILED",
	"lookup-errors" => 'host: eas* AND message: \"Authentication Lookup Error\"',
);

#Hash containing query string for all types of EAS commands
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

#Hash containing all the types of query groups
my %error_types = (
	"eas-commands" => \%eas_commands, 
        "eas-errors" => \%eas_errors,
        "eas-executed-commands" => \%eas_executed_commands,
);

#Hash defining index to be used for each query group
my %index_types = (
	"eas-commands" => "logstash",
	"eas-executed-commands" => "logstash",
	"eas-errors" => "logstash",
);

my $previous_timestamp;
my $current_timestamp;
my $miliseconds=0;

#Setting up logging to log in /var/one.com-eas-dashboard in eas.log
my $logger = setuplogging("EAS");

#If already 2  instances of scripts are running , then terminate and exit from script
my $cron_process = ` ps aux | grep '/usr/bin/perl /usr/local/bin/eas-dashboard.pl --generate-hash' | wc -l`;
chomp($cron_process);
if($cron_process > 4 ){
	$logger->info("Terminating cron as previous 2 crons are still running");
	exit;
}

#If number of instances of scripts in greater than 1, then sleep for 1min than check again till previous instance has completed
while($cron_process > 3){
	$logger->info("Sleeping for 1 min as previous cron is still running ");
	sleep 60;
	$cron_process = `ps aux | grep '/usr/bin/perl /usr/local/bin/eas-dashboard.pl --generate-hash' | wc -l `;
}


my $errorqueryhash;
my ($time,$hash);
GetOptions ("time=s" => \$time,    # numeric
	    "generate-hash" => \$hash,
) 
or warn("Error in command line arguments\n");


#If no argument is provided , then display help message
if( ! defined $hash ){
	print "Filters result from current date ($date)\n";
	print "Usage: eas-dashboard.pl --generate-hash\n";
	print "========================================================================\n";
	print "\n<QUERY_TYPE>\n";
	foreach my $key (keys%error_types){
		print "* $key\n";
	}	
	print "========================================================================\n";
	exit;
}

#Run if generate-hash option is provided 
if(defined $hash){
	#Picking last execution time from eas.timestamp and calculation time range upto current time
	$current_timestamp = `date +%s%N | cut -b1-13`; 
	if( -f "$eas_timestamp_path"){
		$previous_timestamp = `cat $eas_timestamp_path`;
		$logger->info("Last time of successfull execution was $previous_timestamp");
		my $time_range = (($current_timestamp - $previous_timestamp)/(1000*60));
		$logger->info("Querying data for $time_range minutes");
	}
	#Hash containing query strings for requested commands, executed commands and imap hits
	$errorqueryhash = \%eas_commands;
	my $indexname = "$indextype-$date";
        my $query_url = "$logelastic_url/$indexname/_count?pretty";

	#Querying logelastic for each of requested commands, executed commands and imap hits in parallel
        foreach my $errorqueryname ( sort keys%{$errorqueryhash}){
        	#Forming query and getting count for each query type
	        my $errorquery = $errorqueryhash->{$errorqueryname};
	        my $query_data = form_query_data($time,$errorquery,$current_timestamp,$previous_timestamp);
                my $count = get_count($query_url,$query_data);
                $logger->info("$errorqueryname : $count");
		#Forking child process for eacquery type and generating hash
		my $pid = fork();
                if (!defined $pid) {
                    die "Cannot fork: $!";
                }
                elsif ($pid == 0) {
                    $logger->info("Child process for generating hash $errorqueryname..");
                    $0 =  "eas-dashboard --errortype $errorqueryname";
                    generate_hash($indexname,$query_data,$errorqueryname);
                    $logger->info("Child process terminating for $errorqueryname");
                    exit 0;
                }

        }
	#Running simultaneous proces to get count of each eas error types 
        $errorqueryhash = \%eas_errors;
	find_errors_simultaneously();
	
	#Forming sorted arrays from hash for faster extraction of sorted results 
	form_arrays_from_hash();

	#Dumping current timestamp in eas.timestamp and creating .html files for eas-dashboard
	$logger->info("Dumping current timestamp");
        open(my $fh,'>',"$eas_timestamp_path") or die "Cannot open file \n";
        print $fh $current_timestamp;
	my $time = DateTime->now(time_zone => 'Asia/Kolkata');
	my $path = "/var/www/eas-dashboard";
	create_html_dumps($path,$time);
}


#FUNCTIONS

#Fork parallel process for each eas error type and find count of each error 
sub find_errors_simultaneously{
	my $query_url = "$logelastic_url/$indextype-$date/_count?pretty";
 	my $manager = new Parallel::ForkManager(20);
	foreach my $errorqueryname ( sort keys%{$errorqueryhash}){
		$manager->start() and next;
		#Fork child process and form query and get count for each error type andstore value in hash
      		my $errorquery = $errorqueryhash->{$errorqueryname};
	        my $query_data = form_query_data($time,$errorquery,$current_timestamp,$previous_timestamp);
	        my $count = get_count($query_url,$query_data);
		$logger->info("Dumping error type data $errorqueryname \n");
		my $errortype = DBM::Deep->new("$eas_hash_path/errortype.db");
		$errortype->{$errorqueryname} += $count;
		$manager->finish(0);
 	 }
	 $manager->wait_all_children;
}


#Generate hash from eas-logs , forking child process and each process analysing 10,000 logs 
sub generate_hash{
	my $indexname = shift;
	my $query_data = shift;
	my $errortype = shift;
	my $counter = 1;
	
	#Form http request and query logelactic using scroll to process 10,000 logs at one time and scroll id to be used for next 10,000 logs rto  be active for 10minutes
	my $query_url = "$logelastic_url/$indexname/_search?size=10000&scroll=10m";
        my $response_json = http_get_request($query_url,$query_data);
	#extract scroll id from Response code and if scroll size is greater than 0, than perform next query for next 10,000 records
	my $response = from_json($response_json);
	my $scroll_id = $response->{"_scroll_id"};
	my $list = $response->{"hits"}->{"hits"};
	my $scroll_size = scalar @{$list};
	#Fork a child process to anaylse the first 10,000 logs queried from logelastic
	my $pid = fork();
	if (!defined $pid) {
	    die "Cannot fork: $!";
	}
	elsif ($pid == 0) {
	   #code to be executed by child process
	   $logger->info("Child process $counter for analysing batch of size $scroll_size starting for $errortype...");
	   #child process will run with the name initialized in $0
	    $0 =  "eas-dashboard --analysing-batch 1 --errortype $errortype";
	    analyse_batch($list,$errortype);
	    $logger->info("Child process $counter  terminating for $errortype");
	    exit 0;
	}
        $logger->info("Initiating  $counter request for $errortype  and scroll size is $scroll_size");
	#If scroll size is greater than 0, then perform next query with the scroll id
	while($scroll_size > 0){
		$counter++;
        	$query_url = "$logelastic_url/_search/scroll";
		#Form scroll query and then capture response for next 10,000 records
		my $scroll_data = form_scroll_query($scroll_id);
	        $response_json = http_get_request($query_url,$scroll_data);
		$response = from_json($response_json);
		$scroll_id = $response->{"_scroll_id"};
		$list = $response->{"hits"}->{"hits"};
		$scroll_size = scalar @{$list};
		#Find no of processes running for analysing batches and if they are greater than 50 , then sleep for 1min before checking again 
		my $processes = `ps aux | grep 'eas-dashboard --analysing-batch' | grep -v 'Z' | wc -l`;
		while ($processes > 50){
			sleep 60;
			$logger->info("Sleeping for 1min as number of running processes for analysing batch is $processes > 50");
			$processes = `ps aux | grep 'eas-dashboard --analysing-batch' | grep -v 'Z' | wc -l`;
		}	
	
		#Fork child process for analysing queried data
		my $pid = fork();
		if (!defined $pid) {
		    die "Cannot fork: $!";
		}
		elsif ($pid == 0) {
		    $logger->info("Child process $counter for analysing batch of size $scroll_size starting for $errortype...");
		    $0 =  "eas-dashboard --analysing-batch $counter --errortype $errortype";
		    analyse_batch($list,$errortype);
		    $logger->info("Child process $counter terminating for $errortype");
		    exit 0;
		}
        	$logger->info("Initiating  $counter request for $errortype  and scroll size is $scroll_size");
	}

}

sub analyse_batch{
	my $list = shift;
	my $type =shift;
	#Main hash contains data as hash->{"HOSTNAME"}->{"DOMAINNAME"}->{"DEVICEID"}->{"executed/requested"}->{"COMMAND_NAME"}->{"COUNT"}
	my $easdb = DBM::Deep->new("$eas_hash_path/eas.db");
	#Contains mapping {DEVICEID} => {DEVICETYPE} 
	my $devicetype_hash = DBM::Deep->new("$eas_hash_path/devicetype.db");
	#CONTAINS mapping {cph3/wdc} => { COMMAND_NAME} => {COUNT} for requested commands 
	my $requested_command_hash = DBM::Deep->new("$eas_hash_path/requested_command.db");
	#CONTAINS mapping {cph3/wdc} => { COMMAND_NAME} => {COUNT} for executed commands 
	my $executed_command_hash = DBM::Deep->new("$eas_hash_path/executed_command.db");
	#Contains mapping {USER} => {COUNT} for imap hits
	my $imap_hits_hash = DBM::Deep->new("$eas_hash_path/imap-hits.db");
	#Contains mapping {USER} => {COUNT} for imap hits maximum connection exceeded
	my $max_imap_hash = DBM::Deep->new("$eas_hash_path/max-imap-conn.db");
	#Contains mapping {USER} => {COUNT} for requested commands
	my $requested_count = DBM::Deep->new("$eas_hash_path/requested_count.db");
	#Contains mapping {USER} => {COUNT} for executed commands 
	my $executed_count = DBM::Deep->new("$eas_hash_path/executed_count.db");
	#Contains mapping {USER} => {COUNT} for sync executed commands 
	my $sync_count = DBM::Deep->new("$eas_hash_path/sync_count.db");
	#Contains mapping {USER} => {COUNT} for foldersync executed commands 
	my $foldersync_count = DBM::Deep->new("$eas_hash_path/foldersync_count.db");
	#Contains mapping {USER} => {COUNT} for ping executed commands 
	my $ping_count = DBM::Deep->new("$eas_hash_path/ping_count.db");
	#Contains mapping {USER} => {COUNT} for throttled executed commands 
	my $throttled = DBM::Deep->new("$eas_hash_path/throttled.db");

	my $noofdevices = DBM::Deep->new("$eas_hash_path/noofdevices.db");
	$logger->info("I am a analysing batch $type");

	#If eas user logs are analysed ,then fields can be extracted and hash generated 
	#sample data 
	##############################################################
	#{
	#  "_index": "logstash-2017.09.29",
	#  "_type": "fluentd",
	#  "_id": "AV7MYgQhoH0oVDKN6iFm",
	#  "_score": null,
	#  "_source": {
	#    "host": "eas2.cst.appspod1-cph3.one.com",
	#    "ident": "Sep",
	#    "message": "45:27Z eas-wrk-12[75447]:RPC http@3011 [sarhan.salhi@aymax.fr/androidc300468792: C500DBFD-A681-CCAC-7DE734FA-59CDEC06_3B3 RPC] Executing command Ping",
  	#  "facility": "user",
  	#  "severity": "info",
  	#  "@timestamp": "2017-09-29T06:45:27+00:00"
 	# }
	#}
	#################################################################

        if($type eq "executed-commands"){
                foreach my $element ( @$list ){
                        my $host = $element->{_source}->{host};
                        my $message = $element->{_source}->{message};
                        my @fields = split(' ',$message);
                        my $domainfield=$fields[3]; #sarhan.salhi@aymax.fr/androidc300468792
                        $domainfield =~ m/\[(.*)\/(.*):/;
                        my $domain = $1;
                        my $deviceid = $2;
			my $command_type = $fields[5];
		
			if($message =~ "throttle"){
	                        $easdb->{$host}->{$domain}->{$deviceid}->{'throttled'}++;
				$throttled->{$domain}++;
			}
		
			else{
				my $command = $fields[8];
                        	$easdb->{$host}->{$domain}->{$deviceid}->{'executed'}->{$command}++;          
			
				if($command eq "Ping"){
					$ping_count->{$domain}++
				}
				elsif($command eq "Sync"){
					$sync_count->{$domain}++;
				}
				elsif($command eq "FolderSync"){
					$foldersync_count->{$domain}++;
				}
			 	$easdb->{$host}->{$domain}->{'total-executed-command'}++;
				$easdb->{$host}->{'total-executed-command'}++;
				if($host =~ "cph3"){
					$executed_command_hash->{"cph3"}->{$command}++;
				}
				elsif($host =~ "wdc"){
					$executed_command_hash->{"wdc"}->{$command}++;
				}
				$executed_count->{$domain}++;
                	} 
		}
        }

	#If eas local0 logs are analysed to generate hash
	############################################################################
	#{
	#  "_index": "logstash-2017.09.29",
	#  "_type": "fluentd",
	#  "_id": "AV7MZ0ibckKdPZnoiQB7",
	#  "_score": null,
	#  "_source": {
	#    "host": "eas1.cst.appspod1-cph3.one.com",
	#    "ident": "Sep",
	#    "message": "51:10Z access_log[50077]:10.27.4.31 - m.one.com - - - [Fri, 29 Sep 2017 06:51:10 GMT] \"POST /Microsoft-Server-ActiveSync?User=morten.endre@lohne.dk&DeviceId=F4DUUR6LS96ND6TD61TEFK03V8&DeviceType=iPhone&Cmd=Sync HTTP/1.1\" 200 X-Onecom-RID=5431E94C-C09F-CCAC-7DE8B808-59CDED5C_28A 18319 \"-\" \"Apple-iPhone6C2/1501.402\" - http@3002 - 1283.154 ms - 1283.730 ms -  - - - - - - - - morten.endre@lohne.dk - -",
 	#   "facility": "local0",
	#    "severity": "notice",
	#    "@timestamp": "2017-09-29T06:51:10+00:00"
	#  },	
	############################################################################




        elsif($type eq "requested-commands"){
                foreach my $element ( @$list ){
                        my $host = $element->{_source}->{host};
                        my $message = $element->{_source}->{message};
                        my @fields = split(' ',$message);
                        my $requesturl = $fields[14];
			my ($url,$request) = split('\?',$requesturl);  #/Microsoft-Server-ActiveSync?User=morten.endre@lohne.dk&DeviceId=F4DUUR6LS96ND6TD61TEFK03V8&DeviceType=iPhone&Cmd=Sync 
                        my $response_code = $fields[16];	
		        my $domain = $fields[-3];
			my $error = $fields[-1];

			#Sample message with error
			#07:39Z access_log[127894]:10.27.4.31 - m.one.com - - - [Fri, 29 Sep 2017 07:07:39 GMT] "POST /Microsoft-Server-ActiveSync?Cmd=GetItemEstimate&User=Ali%40emruli.de&DeviceId=HTCb6589fc6abdaac72015462&DeviceType=HTCOneA9 HTTP/1.1" 500 X-Onecom-RID=8603552B-D77B-CCAC-7DED19DE-59CDF13B_ED 17 "-" "HTC-EAS-HTCOneA9" - http@3014 - 4.776 ms - 5.022 ms - - - - - - - - - Ali@emruli.de - {"error":"Error","stack":["Error","at /usr/lib/one.com-eas-app/lib/utils/handleEASInactive.js:100:23","at tryCatcher (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/util.js:26:23)","at Promise._resolveFromResolver (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/promise.js:476:31)","at new Promise (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/promise.js:69:37)","at module.exports (/usr/lib/one.com-eas-app/lib/utils/handleEASInactive.js:11:12)","at /usr/lib/one.com-eas-app/lib/middleware/authenticateClient.js:349:17","at tryCatcher (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/util.js:26:23)","at Promise._settlePromiseFromHandler (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/promise.js:503:31)","at Promise._settlePromiseAt (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/promise.js:577:18)","at Promise._settlePromises (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/promise.js:693:14)","at Async._drainQueue (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/async.js:123:16)","at Async._drainQueues (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/async.js:133:10)","at Immediate.Async.drainQueues (/usr/lib/one.com-eas-app/node_modules/bluebird/js/main/async.js:15:14)","at runCallback (timers.js:672:20)","at tryOnImmediate (timers.js:645:5)","at processImmediate [as _immediateCallback] (timers.js:617:5)"]}

			#If no errors, then error eq "-"
			if($error ne "-"){
				#Removing error part from message and then extracting domain name
				if($message =~ s/\{"error".*//){
					my @arr = split(' ',$message);
					$domain = $arr[-2];
				}
				else{
					$logger->info("dummy data error $message");				
				}
			}

			#if domain is not defined , as in case of OPTIONS command , move to next
			if(! defined $domain  || $domain eq '-'){
				next;
			}
			# Sends user as domain.tld\username or domain.tld\username@domain.tld.
			if($domain =~ m/(.*)%5C(.*)/){
				my $first = $1;
				my $second = $2;
				if($second =~ m/@/){
					$domain = $second; 
				}
				else{
					$domain = "${second}\@${first}";
				}

			}
			#Sometimes domain names are prepended with /
			$domain =~ s/^\///g;
			my ($command,$devicetype,$deviceid) ;
			#request = User=morten.endre@lohne.dk&DeviceId=F4DUUR6LS96ND6TD61TEFK03V8&DeviceType=iPhone&Cmd=Sync
			if(defined $request){
		        	if($request =~ m/Cmd=(.*?)(&|$)/){
                      			 $command = $1;
              			}
		        	if($request =~ m/DeviceType=(.*?)(&|$)/){
                      			 $devicetype = $1;
                       		 }
				if($request =~ m/DeviceId=(.*?)(&|$)/){
        	                	 $deviceid = $1;
				}
				$easdb->{$host}->{'total-requested-command'}++;
        	        	
					#some devices send base64 encoded strings
					#sample message
					# 	17:07Z access_log[50088]:10.27.4.31 - m.one.com - - - [Fri, 29 Sep 2017 07:17:07 GMT] "POST /Microsoft-Server-ActiveSync?jAAMBBBFO4PTgVN5IoizMDNebAlhBAAAAAALV2luZG93c01haWw= HTTP/1.1" 200 X-Onecom-RID=6D1FFE05-C796-CCAC-7DEF8C5A-59CDF372_326 183 "-" "-" - http@3013 - 275.908 ms - 276.308 ms -  - - - - - - - - richard@joubert.one - -

				if(! defined $deviceid && ! defined $command && ! defined $devicetype ){
					($command,$deviceid,$devicetype) = handle_base64_encoded($request);
					#in case base64 encoding throws some error, move to next log line
					if($command eq "0"){
						next;
					}	
				}	
		
				#If everything is correctly recieved then initialize the hashes 		
				if(defined $devicetype && defined $deviceid && defined $command){
					if(! defined $devicetype_hash->{$deviceid}){
						$noofdevices->{$domain}++;
						$devicetype_hash->{'total'}++;
					}
					$devicetype_hash->{$deviceid} = $devicetype;
					$easdb->{$host}->{$domain}->{$deviceid}->{'requested'}->{$command}++;
       	        		        $easdb->{$host}->{$domain}->{'total-requested-command'}++;
					if(! defined  $easdb->{$host}->{$domain}->{'total-executed-command'}){
						$easdb->{$host}->{$domain}->{'total-executed-command'} = 0;
					}
					if($host =~ "cph3"){
						$requested_command_hash->{"cph3"}->{$command}++;
					}
					elsif($host =~ "wdc"){
						$requested_command_hash->{"wdc"}->{$command}++;	
					}
					$requested_count->{$domain}++;
				}
			}
		}
       }

	#If logs analysed are mailstorage logs imap login 
	#sample message
	###################################################################################################################################
	#{
	#  "_index": "logstash-2017.09.29",
	#  "_type": "fluentd",
	#  "_id": "AV7MhmRIckKdPZno6KdX",
	#  "_score": null,
	#  "_source": {
	#    "host": "mail216.local.one.com",
	#    "ident": "dovecot",
	#    "message": "imap-login: Login: user=<bart.rubens@arthal.be>, method=PLAIN, rip=46.30.211.97, rp=60390, lip=46.30.211.92, mpid=28173, secured",
	#    "facility": "mail",
	#    "severity": "info",
	#    "@timestamp": "2017-09-29T07:25:12+00:00"
	#  },
	###################################################################################################################################



	elsif($type eq "imap-hits"){
		 foreach my $element ( @$list ){
                        my $host = $element->{_source}->{host};
			my $message = $element->{_source}->{message};
                        my @fields = split(' ',$message);
                        my $request = $fields[2];
			if($request =~ /user=<(.*)>/){
				my $user = $1;
				$imap_hits_hash->{$user}++;
			}
			# Maximum number of connections tracking
			#sample message
			#  imap-login: Maximum number of connections from user+IP exceeded (mail_max_userip_connections=25): user=<cto@the2ndedit.com>, method=PLAIN, rip=212.27.21.78, rp=16377, lip=46.30.211.111, secured
			elsif($request eq "number"){
				$request = $fields[9];
				if($request =~ /user=<(.*)>/){
                         	       my $user = $1;
                                       $max_imap_hash->{$user}++;
                       		 }

			}

		}

	}
}


#function fort forming arrays from hashes for each of the types by forking child for each type
sub form_arrays_from_hash{
	my @types = ("EXECUTED","REQUESTED","SYNC","FOLDERSYNC","PING","DEVICETYPE","IMAP-HITS","MAX-IMAP","DEVICES-COUNT");
	foreach my $type ( @types){
		my $pid = fork();
		if (!defined $pid) {
		    die "Cannot fork: $!";
		}
		elsif ($pid == 0) {
		    $logger->info("Child process starting for $type..");
	  	    $0 = "eas-dashboard --form-array $type";
		    form_array($type);
		    $logger->info("Child process terminating for $type");
		    exit 0;
		}
	}
}


#Creates a sorted array from hash for each of the type
# Hash  {USER} => {COUNT}
#Array [COUNT] = [USER1, USER2, USER3 ]

sub form_array{
	my $type = shift;
	my $array ;
	my $hash;
	if($type eq "EXECUTED"){
		`rm $eas_hash_path/executed.db`;
		$array = DBM::Deep->new(
			file => "$eas_hash_path/executed.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/executed_count.db");
	}
	elsif($type eq "REQUESTED"){		
		`rm $eas_hash_path/requested.db`;
		$array = DBM::Deep->new(
			file => "$eas_hash_path/requested.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/requested_count.db");
	}
	elsif($type eq "FOLDERSYNC"){			
		`rm $eas_hash_path/foldersync.db`;
		$array = DBM::Deep->new(
			file => "$eas_hash_path/foldersync.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash= DBM::Deep->new("$eas_hash_path/foldersync_count.db");
	}
	elsif($type eq "SYNC"){			
		`rm $eas_hash_path/sync.db`;
		$array = DBM::Deep->new(
			file => "$eas_hash_path/sync.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash= DBM::Deep->new("$eas_hash_path/sync_count.db");
	}
	elsif($type eq "PING"){
		`rm $eas_hash_path/ping.db`;			
		$array = DBM::Deep->new(
			file => "$eas_hash_path/ping.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/ping_count.db");
	}
	elsif($type eq "IMAP-HITS"){
		`rm $eas_hash_path/imap.db`;			
		$array = DBM::Deep->new(
			file => "$eas_hash_path/imap.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/imap-hits.db");
	}
	elsif($type eq "MAX-IMAP"){
		`rm $eas_hash_path/max-imap.db`;			
		$array = DBM::Deep->new(
			file => "$eas_hash_path/max-imap.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/max-imap-conn.db");
	}
	elsif($type eq "DEVICES-COUNT"){
		`rm $eas_hash_path/noofdevices_array.db`;			
		$array = DBM::Deep->new(
			file => "$eas_hash_path/noofdevices_array.db",
			type => DBM::Deep->TYPE_ARRAY
		);
		$hash = DBM::Deep->new("$eas_hash_path/noofdevices.db");
	}
	elsif($type eq "DEVICETYPE"){
	
		my $device = DBM::Deep->new(
			file => "$eas_hash_path/devices_temp.db",
		);
		# HASH has {DEVICEID} => {DEVICETYPE}
		$hash = DBM::Deep->new("$eas_hash_path/devicetype.db");
		#Forming hash {DEVICETYPR} => {COUNT}
		foreach my $value ( values % $hash ){
			$device->{$value}++;
		}
		`mv $eas_hash_path/devices_temp.db  $eas_hash_path/devices.db`;
		return;
	
	}
	
	while (my ($key,$value) = each %$hash) {
            	if( ! defined $value){
			$logger->info("Error!! $key does not have a value in $type hash");
		}
		else{
			push(@{$array->[$value]},$key);
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

#Query logelastic nodes by HTTP GET and get count from response json
sub get_count{
	my $query_url = shift;
	my $query_data = shift;
	my $response_json = http_get_request($query_url,$query_data);
	my $response = from_json($response_json);
	my $count = $response->{"count"};
	return $count;
}

#Send HTTP request to given url with query data
sub http_get_request{
	my $url =shift;
	my $query_data = shift;
	my $ua = LWP::UserAgent->new;
	$ua->credentials("kibana.one.com:9200","Protected Access to OneComElasticSearch","easdev","one.com123");
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

#Form scroll query from given scroll id
sub form_scroll_query{
	my $scroll_id = shift;
	my $query_data =  <<"END_MSG";
{
    "scroll" : "10m", 
    "scroll_id" : "$scroll_id" 
}
END_MSG
}

#Form query  data for querying logelastic
sub form_query_data{
	my $time = shift;
	my $query = shift;
	my $current_ts = shift;
	my $previous_ts =shift;
	my $query_data;

#If not defined option time and current/previous timestamps defined 
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
	#If only current timestamp defined not previous timestamp
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
	#If no timestamps are defined
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
#If user specifies time using command line options
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

sub create_html_dumps{
	 my $path = shift;
	 my $time = shift;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --command --time $time > $path/commands-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --request --time $time > $path/requests-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --error --time $time > $path/errors-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --device --time $time > $path/devices-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --device --time $time > $path/devices-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --imap --time $time > $path/imap-stat.html`;
	`perl /var/www/eas-dashboard/eas-query-hash.pl --customer --time $time > $path/customer-stat.html`;
}

sub handle_base64_encoded{
	my $base64 = shift;
	my $json = `node /usr/local/bin/parse $base64`;
	
	if($json =~ /RangeError/){
		return 0;
	}
	my $decoded = decode_json($json);
	my $cmd = $decoded->{'Cmd'};
	my $deviceid = $decoded->{'DeviceId'};
	my $devicetype = $decoded->{'DeviceType'};
	return ($cmd,$deviceid,$devicetype);
}
