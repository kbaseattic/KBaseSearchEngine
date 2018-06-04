package KBaseSearchEngine::KBaseSearchEngineClient;

use JSON::RPC::Client;
use POSIX;
use strict;
use Data::Dumper;
use URI;
use Bio::KBase::Exceptions;
my $get_time = sub { time, 0 };
eval {
    require Time::HiRes;
    $get_time = sub { Time::HiRes::gettimeofday() };
};

use Bio::KBase::AuthToken;

# Client version should match Impl version
# This is a Semantic Version number,
# http://semver.org
our $VERSION = "0.1.0";

=head1 NAME

KBaseSearchEngine::KBaseSearchEngineClient

=head1 DESCRIPTION





=cut

sub new
{
    my($class, $url, @args) = @_;
    

    my $self = {
	client => KBaseSearchEngine::KBaseSearchEngineClient::RpcClient->new,
	url => $url,
	headers => [],
    };

    chomp($self->{hostname} = `hostname`);
    $self->{hostname} ||= 'unknown-host';

    #
    # Set up for propagating KBRPC_TAG and KBRPC_METADATA environment variables through
    # to invoked services. If these values are not set, we create a new tag
    # and a metadata field with basic information about the invoking script.
    #
    if ($ENV{KBRPC_TAG})
    {
	$self->{kbrpc_tag} = $ENV{KBRPC_TAG};
    }
    else
    {
	my ($t, $us) = &$get_time();
	$us = sprintf("%06d", $us);
	my $ts = strftime("%Y-%m-%dT%H:%M:%S.${us}Z", gmtime $t);
	$self->{kbrpc_tag} = "C:$0:$self->{hostname}:$$:$ts";
    }
    push(@{$self->{headers}}, 'Kbrpc-Tag', $self->{kbrpc_tag});

    if ($ENV{KBRPC_METADATA})
    {
	$self->{kbrpc_metadata} = $ENV{KBRPC_METADATA};
	push(@{$self->{headers}}, 'Kbrpc-Metadata', $self->{kbrpc_metadata});
    }

    if ($ENV{KBRPC_ERROR_DEST})
    {
	$self->{kbrpc_error_dest} = $ENV{KBRPC_ERROR_DEST};
	push(@{$self->{headers}}, 'Kbrpc-Errordest', $self->{kbrpc_error_dest});
    }

    #
    # This module requires authentication.
    #
    # We create an auth token, passing through the arguments that we were (hopefully) given.

    {
	my %arg_hash2 = @args;
	if (exists $arg_hash2{"token"}) {
	    $self->{token} = $arg_hash2{"token"};
	} elsif (exists $arg_hash2{"user_id"}) {
	    my $token = Bio::KBase::AuthToken->new(@args);
	    if (!$token->error_message) {
	        $self->{token} = $token->token;
	    }
	}
	
	if (exists $self->{token})
	{
	    $self->{client}->{token} = $self->{token};
	}
    }

    my $ua = $self->{client}->ua;	 
    my $timeout = $ENV{CDMI_TIMEOUT} || (30 * 60);	 
    $ua->timeout($timeout);
    bless $self, $class;
    #    $self->_validate_version();
    return $self;
}




=head2 search_types

  $return = $obj->search_types($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a KBaseSearchEngine.SearchTypesInput
$return is a KBaseSearchEngine.SearchTypesOutput
SearchTypesInput is a reference to a hash where the following keys are defined:
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
	access_filter has a value which is a KBaseSearchEngine.AccessFilter
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
boolean is an int
AccessFilter is a reference to a hash where the following keys are defined:
	with_private has a value which is a KBaseSearchEngine.boolean
	with_public has a value which is a KBaseSearchEngine.boolean
	with_all_history has a value which is a KBaseSearchEngine.boolean
SearchTypesOutput is a reference to a hash where the following keys are defined:
	type_to_count has a value which is a reference to a hash where the key is a string and the value is an int
	search_time has a value which is an int

</pre>

=end html

=begin text

$params is a KBaseSearchEngine.SearchTypesInput
$return is a KBaseSearchEngine.SearchTypesOutput
SearchTypesInput is a reference to a hash where the following keys are defined:
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
	access_filter has a value which is a KBaseSearchEngine.AccessFilter
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
boolean is an int
AccessFilter is a reference to a hash where the following keys are defined:
	with_private has a value which is a KBaseSearchEngine.boolean
	with_public has a value which is a KBaseSearchEngine.boolean
	with_all_history has a value which is a KBaseSearchEngine.boolean
SearchTypesOutput is a reference to a hash where the following keys are defined:
	type_to_count has a value which is a reference to a hash where the key is a string and the value is an int
	search_time has a value which is an int


=end text

=item Description

Search for number of objects of each type matching constraints.

=back

=cut

 sub search_types
{
    my($self, @args) = @_;

# Authentication: required

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function search_types (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to search_types:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'search_types');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "KBaseSearchEngine.search_types",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'search_types',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method search_types",
					    status_line => $self->{client}->status_line,
					    method_name => 'search_types',
				       );
    }
}
 


=head2 search_objects

  $return = $obj->search_objects($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a KBaseSearchEngine.SearchObjectsInput
$return is a KBaseSearchEngine.SearchObjectsOutput
SearchObjectsInput is a reference to a hash where the following keys are defined:
	object_types has a value which is a reference to a list where each element is a string
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
	sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
	access_filter has a value which is a KBaseSearchEngine.AccessFilter
	pagination has a value which is a KBaseSearchEngine.Pagination
	post_processing has a value which is a KBaseSearchEngine.PostProcessing
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
boolean is an int
SortingRule is a reference to a hash where the following keys are defined:
	property has a value which is a string
	is_object_property has a value which is a KBaseSearchEngine.boolean
	ascending has a value which is a KBaseSearchEngine.boolean
AccessFilter is a reference to a hash where the following keys are defined:
	with_private has a value which is a KBaseSearchEngine.boolean
	with_public has a value which is a KBaseSearchEngine.boolean
	with_all_history has a value which is a KBaseSearchEngine.boolean
Pagination is a reference to a hash where the following keys are defined:
	start has a value which is an int
	count has a value which is an int
PostProcessing is a reference to a hash where the following keys are defined:
	ids_only has a value which is a KBaseSearchEngine.boolean
	skip_keys has a value which is a KBaseSearchEngine.boolean
	skip_data has a value which is a KBaseSearchEngine.boolean
	include_highlight has a value which is a KBaseSearchEngine.boolean
	add_narrative_info has a value which is a KBaseSearchEngine.boolean
	add_access_group_info has a value which is a KBaseSearchEngine.boolean
SearchObjectsOutput is a reference to a hash where the following keys are defined:
	pagination has a value which is a KBaseSearchEngine.Pagination
	sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
	objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
	total has a value which is an int
	search_time has a value which is an int
	access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
	access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
	objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a KBaseSearchEngine.object_info
ObjectData is a reference to a hash where the following keys are defined:
	guid has a value which is a KBaseSearchEngine.GUID
	parent_guid has a value which is a KBaseSearchEngine.GUID
	object_name has a value which is a string
	timestamp has a value which is an int
	type has a value which is a string
	type_ver has a value which is an int
	creator has a value which is a string
	copier has a value which is a string
	mod has a value which is a string
	method has a value which is a string
	module_ver has a value which is a string
	commit has a value which is a string
	parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
	data has a value which is an UnspecifiedObject, which can hold any non-null object
	key_props has a value which is a reference to a hash where the key is a string and the value is a string
	highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string
GUID is a string
access_group_id is an int
narrative_info is a reference to a list containing 5 items:
	0: (narrative_name) a string
	1: (narrative_id) an int
	2: (time_last_saved) a KBaseSearchEngine.timestamp
	3: (ws_owner_username) a string
	4: (ws_owner_displayname) a string
timestamp is an int
access_group_info is a Workspace.workspace_info
workspace_info is a reference to a list containing 9 items:
	0: (id) a Workspace.ws_id
	1: (workspace) a Workspace.ws_name
	2: (owner) a Workspace.username
	3: (moddate) a Workspace.timestamp
	4: (max_objid) an int
	5: (user_permission) a Workspace.permission
	6: (globalread) a Workspace.permission
	7: (lockstat) a Workspace.lock_status
	8: (metadata) a Workspace.usermeta
ws_id is an int
ws_name is a string
username is a string
permission is a string
lock_status is a string
usermeta is a reference to a hash where the key is a string and the value is a string
obj_ref is a string
object_info is a Workspace.object_info

</pre>

=end html

=begin text

$params is a KBaseSearchEngine.SearchObjectsInput
$return is a KBaseSearchEngine.SearchObjectsOutput
SearchObjectsInput is a reference to a hash where the following keys are defined:
	object_types has a value which is a reference to a list where each element is a string
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
	sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
	access_filter has a value which is a KBaseSearchEngine.AccessFilter
	pagination has a value which is a KBaseSearchEngine.Pagination
	post_processing has a value which is a KBaseSearchEngine.PostProcessing
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
boolean is an int
SortingRule is a reference to a hash where the following keys are defined:
	property has a value which is a string
	is_object_property has a value which is a KBaseSearchEngine.boolean
	ascending has a value which is a KBaseSearchEngine.boolean
AccessFilter is a reference to a hash where the following keys are defined:
	with_private has a value which is a KBaseSearchEngine.boolean
	with_public has a value which is a KBaseSearchEngine.boolean
	with_all_history has a value which is a KBaseSearchEngine.boolean
Pagination is a reference to a hash where the following keys are defined:
	start has a value which is an int
	count has a value which is an int
PostProcessing is a reference to a hash where the following keys are defined:
	ids_only has a value which is a KBaseSearchEngine.boolean
	skip_keys has a value which is a KBaseSearchEngine.boolean
	skip_data has a value which is a KBaseSearchEngine.boolean
	include_highlight has a value which is a KBaseSearchEngine.boolean
	add_narrative_info has a value which is a KBaseSearchEngine.boolean
	add_access_group_info has a value which is a KBaseSearchEngine.boolean
SearchObjectsOutput is a reference to a hash where the following keys are defined:
	pagination has a value which is a KBaseSearchEngine.Pagination
	sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
	objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
	total has a value which is an int
	search_time has a value which is an int
	access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
	access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
	objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a KBaseSearchEngine.object_info
ObjectData is a reference to a hash where the following keys are defined:
	guid has a value which is a KBaseSearchEngine.GUID
	parent_guid has a value which is a KBaseSearchEngine.GUID
	object_name has a value which is a string
	timestamp has a value which is an int
	type has a value which is a string
	type_ver has a value which is an int
	creator has a value which is a string
	copier has a value which is a string
	mod has a value which is a string
	method has a value which is a string
	module_ver has a value which is a string
	commit has a value which is a string
	parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
	data has a value which is an UnspecifiedObject, which can hold any non-null object
	key_props has a value which is a reference to a hash where the key is a string and the value is a string
	highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string
GUID is a string
access_group_id is an int
narrative_info is a reference to a list containing 5 items:
	0: (narrative_name) a string
	1: (narrative_id) an int
	2: (time_last_saved) a KBaseSearchEngine.timestamp
	3: (ws_owner_username) a string
	4: (ws_owner_displayname) a string
timestamp is an int
access_group_info is a Workspace.workspace_info
workspace_info is a reference to a list containing 9 items:
	0: (id) a Workspace.ws_id
	1: (workspace) a Workspace.ws_name
	2: (owner) a Workspace.username
	3: (moddate) a Workspace.timestamp
	4: (max_objid) an int
	5: (user_permission) a Workspace.permission
	6: (globalread) a Workspace.permission
	7: (lockstat) a Workspace.lock_status
	8: (metadata) a Workspace.usermeta
ws_id is an int
ws_name is a string
username is a string
permission is a string
lock_status is a string
usermeta is a reference to a hash where the key is a string and the value is a string
obj_ref is a string
object_info is a Workspace.object_info


=end text

=item Description

Search for objects of particular type matching constraints.

=back

=cut

 sub search_objects
{
    my($self, @args) = @_;

# Authentication: required

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function search_objects (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to search_objects:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'search_objects');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "KBaseSearchEngine.search_objects",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'search_objects',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method search_objects",
					    status_line => $self->{client}->status_line,
					    method_name => 'search_objects',
				       );
    }
}
 


=head2 get_objects

  $return = $obj->get_objects($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a KBaseSearchEngine.GetObjectsInput
$return is a KBaseSearchEngine.GetObjectsOutput
GetObjectsInput is a reference to a hash where the following keys are defined:
	guids has a value which is a reference to a list where each element is a KBaseSearchEngine.GUID
	post_processing has a value which is a KBaseSearchEngine.PostProcessing
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
GUID is a string
PostProcessing is a reference to a hash where the following keys are defined:
	ids_only has a value which is a KBaseSearchEngine.boolean
	skip_keys has a value which is a KBaseSearchEngine.boolean
	skip_data has a value which is a KBaseSearchEngine.boolean
	include_highlight has a value which is a KBaseSearchEngine.boolean
	add_narrative_info has a value which is a KBaseSearchEngine.boolean
	add_access_group_info has a value which is a KBaseSearchEngine.boolean
boolean is an int
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
GetObjectsOutput is a reference to a hash where the following keys are defined:
	objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
	search_time has a value which is an int
	access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
	access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
	objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a Workspace.object_info
ObjectData is a reference to a hash where the following keys are defined:
	guid has a value which is a KBaseSearchEngine.GUID
	parent_guid has a value which is a KBaseSearchEngine.GUID
	object_name has a value which is a string
	timestamp has a value which is an int
	type has a value which is a string
	type_ver has a value which is an int
	creator has a value which is a string
	copier has a value which is a string
	mod has a value which is a string
	method has a value which is a string
	module_ver has a value which is a string
	commit has a value which is a string
	parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
	data has a value which is an UnspecifiedObject, which can hold any non-null object
	key_props has a value which is a reference to a hash where the key is a string and the value is a string
	highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string
access_group_id is an int
narrative_info is a reference to a list containing 5 items:
	0: (narrative_name) a string
	1: (narrative_id) an int
	2: (time_last_saved) a KBaseSearchEngine.timestamp
	3: (ws_owner_username) a string
	4: (ws_owner_displayname) a string
timestamp is an int
access_group_info is a Workspace.workspace_info
workspace_info is a reference to a list containing 9 items:
	0: (id) a Workspace.ws_id
	1: (workspace) a Workspace.ws_name
	2: (owner) a Workspace.username
	3: (moddate) a Workspace.timestamp
	4: (max_objid) an int
	5: (user_permission) a Workspace.permission
	6: (globalread) a Workspace.permission
	7: (lockstat) a Workspace.lock_status
	8: (metadata) a Workspace.usermeta
ws_id is an int
ws_name is a string
username is a string
permission is a string
lock_status is a string
usermeta is a reference to a hash where the key is a string and the value is a string
obj_ref is a string
object_info is a reference to a list containing 11 items:
	0: (objid) a Workspace.obj_id
	1: (name) a Workspace.obj_name
	2: (type) a Workspace.type_string
	3: (save_date) a Workspace.timestamp
	4: (version) an int
	5: (saved_by) a Workspace.username
	6: (wsid) a Workspace.ws_id
	7: (workspace) a Workspace.ws_name
	8: (chsum) a string
	9: (size) an int
	10: (meta) a Workspace.usermeta
obj_id is an int
obj_name is a string
type_string is a string

</pre>

=end html

=begin text

$params is a KBaseSearchEngine.GetObjectsInput
$return is a KBaseSearchEngine.GetObjectsOutput
GetObjectsInput is a reference to a hash where the following keys are defined:
	guids has a value which is a reference to a list where each element is a KBaseSearchEngine.GUID
	post_processing has a value which is a KBaseSearchEngine.PostProcessing
	match_filter has a value which is a KBaseSearchEngine.MatchFilter
GUID is a string
PostProcessing is a reference to a hash where the following keys are defined:
	ids_only has a value which is a KBaseSearchEngine.boolean
	skip_keys has a value which is a KBaseSearchEngine.boolean
	skip_data has a value which is a KBaseSearchEngine.boolean
	include_highlight has a value which is a KBaseSearchEngine.boolean
	add_narrative_info has a value which is a KBaseSearchEngine.boolean
	add_access_group_info has a value which is a KBaseSearchEngine.boolean
boolean is an int
MatchFilter is a reference to a hash where the following keys are defined:
	full_text_in_all has a value which is a string
	object_name has a value which is a string
	timestamp has a value which is a KBaseSearchEngine.MatchValue
	exclude_subobjects has a value which is a KBaseSearchEngine.boolean
	lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
	source_tags has a value which is a reference to a list where each element is a string
	source_tags_blacklist has a value which is a KBaseSearchEngine.boolean
MatchValue is a reference to a hash where the following keys are defined:
	value has a value which is a string
	int_value has a value which is an int
	double_value has a value which is a float
	bool_value has a value which is a KBaseSearchEngine.boolean
	min_int has a value which is an int
	max_int has a value which is an int
	min_date has a value which is an int
	max_date has a value which is an int
	min_double has a value which is a float
	max_double has a value which is a float
GetObjectsOutput is a reference to a hash where the following keys are defined:
	objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
	search_time has a value which is an int
	access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
	access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
	objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a Workspace.object_info
ObjectData is a reference to a hash where the following keys are defined:
	guid has a value which is a KBaseSearchEngine.GUID
	parent_guid has a value which is a KBaseSearchEngine.GUID
	object_name has a value which is a string
	timestamp has a value which is an int
	type has a value which is a string
	type_ver has a value which is an int
	creator has a value which is a string
	copier has a value which is a string
	mod has a value which is a string
	method has a value which is a string
	module_ver has a value which is a string
	commit has a value which is a string
	parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
	data has a value which is an UnspecifiedObject, which can hold any non-null object
	key_props has a value which is a reference to a hash where the key is a string and the value is a string
	highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string
access_group_id is an int
narrative_info is a reference to a list containing 5 items:
	0: (narrative_name) a string
	1: (narrative_id) an int
	2: (time_last_saved) a KBaseSearchEngine.timestamp
	3: (ws_owner_username) a string
	4: (ws_owner_displayname) a string
timestamp is an int
access_group_info is a Workspace.workspace_info
workspace_info is a reference to a list containing 9 items:
	0: (id) a Workspace.ws_id
	1: (workspace) a Workspace.ws_name
	2: (owner) a Workspace.username
	3: (moddate) a Workspace.timestamp
	4: (max_objid) an int
	5: (user_permission) a Workspace.permission
	6: (globalread) a Workspace.permission
	7: (lockstat) a Workspace.lock_status
	8: (metadata) a Workspace.usermeta
ws_id is an int
ws_name is a string
username is a string
permission is a string
lock_status is a string
usermeta is a reference to a hash where the key is a string and the value is a string
obj_ref is a string
object_info is a reference to a list containing 11 items:
	0: (objid) a Workspace.obj_id
	1: (name) a Workspace.obj_name
	2: (type) a Workspace.type_string
	3: (save_date) a Workspace.timestamp
	4: (version) an int
	5: (saved_by) a Workspace.username
	6: (wsid) a Workspace.ws_id
	7: (workspace) a Workspace.ws_name
	8: (chsum) a string
	9: (size) an int
	10: (meta) a Workspace.usermeta
obj_id is an int
obj_name is a string
type_string is a string


=end text

=item Description

Retrieve objects by their GUIDs.

=back

=cut

 sub get_objects
{
    my($self, @args) = @_;

# Authentication: required

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function get_objects (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to get_objects:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'get_objects');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "KBaseSearchEngine.get_objects",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'get_objects',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method get_objects",
					    status_line => $self->{client}->status_line,
					    method_name => 'get_objects',
				       );
    }
}
 


=head2 list_types

  $return = $obj->list_types($params)

=over 4

=item Parameter and return types

=begin html

<pre>
$params is a KBaseSearchEngine.ListTypesInput
$return is a KBaseSearchEngine.ListTypesOutput
ListTypesInput is a reference to a hash where the following keys are defined:
	type_name has a value which is a string
ListTypesOutput is a reference to a hash where the following keys are defined:
	types has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.TypeDescriptor
TypeDescriptor is a reference to a hash where the following keys are defined:
	type_name has a value which is a string
	type_ui_title has a value which is a string
	keys has a value which is a reference to a list where each element is a KBaseSearchEngine.KeyDescription
KeyDescription is a reference to a hash where the following keys are defined:
	key_name has a value which is a string
	key_ui_title has a value which is a string
	key_value_type has a value which is a string
	hidden has a value which is a KBaseSearchEngine.boolean
	link_key has a value which is a string
boolean is an int

</pre>

=end html

=begin text

$params is a KBaseSearchEngine.ListTypesInput
$return is a KBaseSearchEngine.ListTypesOutput
ListTypesInput is a reference to a hash where the following keys are defined:
	type_name has a value which is a string
ListTypesOutput is a reference to a hash where the following keys are defined:
	types has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.TypeDescriptor
TypeDescriptor is a reference to a hash where the following keys are defined:
	type_name has a value which is a string
	type_ui_title has a value which is a string
	keys has a value which is a reference to a list where each element is a KBaseSearchEngine.KeyDescription
KeyDescription is a reference to a hash where the following keys are defined:
	key_name has a value which is a string
	key_ui_title has a value which is a string
	key_value_type has a value which is a string
	hidden has a value which is a KBaseSearchEngine.boolean
	link_key has a value which is a string
boolean is an int


=end text

=item Description

List registered searchable object types.

=back

=cut

 sub list_types
{
    my($self, @args) = @_;

# Authentication: none

    if ((my $n = @args) != 1)
    {
	Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
							       "Invalid argument count for function list_types (received $n, expecting 1)");
    }
    {
	my($params) = @args;

	my @_bad_arguments;
        (ref($params) eq 'HASH') or push(@_bad_arguments, "Invalid type for argument 1 \"params\" (value was \"$params\")");
        if (@_bad_arguments) {
	    my $msg = "Invalid arguments passed to list_types:\n" . join("", map { "\t$_\n" } @_bad_arguments);
	    Bio::KBase::Exceptions::ArgumentValidationError->throw(error => $msg,
								   method_name => 'list_types');
	}
    }

    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
	    method => "KBaseSearchEngine.list_types",
	    params => \@args,
    });
    if ($result) {
	if ($result->is_error) {
	    Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
					       code => $result->content->{error}->{code},
					       method_name => 'list_types',
					       data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
					      );
	} else {
	    return wantarray ? @{$result->result} : $result->result->[0];
	}
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method list_types",
					    status_line => $self->{client}->status_line,
					    method_name => 'list_types',
				       );
    }
}
 
  
sub status
{
    my($self, @args) = @_;
    if ((my $n = @args) != 0) {
        Bio::KBase::Exceptions::ArgumentValidationError->throw(error =>
                                   "Invalid argument count for function status (received $n, expecting 0)");
    }
    my $url = $self->{url};
    my $result = $self->{client}->call($url, $self->{headers}, {
        method => "KBaseSearchEngine.status",
        params => \@args,
    });
    if ($result) {
        if ($result->is_error) {
            Bio::KBase::Exceptions::JSONRPC->throw(error => $result->error_message,
                           code => $result->content->{error}->{code},
                           method_name => 'status',
                           data => $result->content->{error}->{error} # JSON::RPC::ReturnObject only supports JSONRPC 1.1 or 1.O
                          );
        } else {
            return wantarray ? @{$result->result} : $result->result->[0];
        }
    } else {
        Bio::KBase::Exceptions::HTTP->throw(error => "Error invoking method status",
                        status_line => $self->{client}->status_line,
                        method_name => 'status',
                       );
    }
}
   

sub version {
    my ($self) = @_;
    my $result = $self->{client}->call($self->{url}, $self->{headers}, {
        method => "KBaseSearchEngine.version",
        params => [],
    });
    if ($result) {
        if ($result->is_error) {
            Bio::KBase::Exceptions::JSONRPC->throw(
                error => $result->error_message,
                code => $result->content->{code},
                method_name => 'list_types',
            );
        } else {
            return wantarray ? @{$result->result} : $result->result->[0];
        }
    } else {
        Bio::KBase::Exceptions::HTTP->throw(
            error => "Error invoking method list_types",
            status_line => $self->{client}->status_line,
            method_name => 'list_types',
        );
    }
}

sub _validate_version {
    my ($self) = @_;
    my $svr_version = $self->version();
    my $client_version = $VERSION;
    my ($cMajor, $cMinor) = split(/\./, $client_version);
    my ($sMajor, $sMinor) = split(/\./, $svr_version);
    if ($sMajor != $cMajor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Major version numbers differ.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor < $cMinor) {
        Bio::KBase::Exceptions::ClientServerIncompatible->throw(
            error => "Client minor version greater than Server minor version.",
            server_version => $svr_version,
            client_version => $client_version
        );
    }
    if ($sMinor > $cMinor) {
        warn "New client version available for KBaseSearchEngine::KBaseSearchEngineClient\n";
    }
    if ($sMajor == 0) {
        warn "KBaseSearchEngine::KBaseSearchEngineClient version is $svr_version. API subject to change.\n";
    }
}

=head1 TYPES



=head2 boolean

=over 4



=item Description

A boolean. 0 = false, other = true.


=item Definition

=begin html

<pre>
an int
</pre>

=end html

=begin text

an int

=end text

=back



=head2 obj_ref

=over 4



=item Description

An X/Y/Z style reference


=item Definition

=begin html

<pre>
a string
</pre>

=end html

=begin text

a string

=end text

=back



=head2 GUID

=over 4



=item Description

Global user identificator. It has structure like this:
  <data-source-code>:<full-reference>[:<sub-type>/<sub-id>]


=item Definition

=begin html

<pre>
a string
</pre>

=end html

=begin text

a string

=end text

=back



=head2 MatchValue

=over 4



=item Description

Optional rules of defining constraints for values of particular
term (keyword). Appropriate field depends on type of keyword.
For instance in case of integer type 'int_value' should be used.
In case of range constraint rather than single value 'min_*' 
and 'max_*' fields should be used. You may omit one of ends of
range to achieve '<=' or '>=' comparison. Ends are always
included for range constraints.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
value has a value which is a string
int_value has a value which is an int
double_value has a value which is a float
bool_value has a value which is a KBaseSearchEngine.boolean
min_int has a value which is an int
max_int has a value which is an int
min_date has a value which is an int
max_date has a value which is an int
min_double has a value which is a float
max_double has a value which is a float

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
value has a value which is a string
int_value has a value which is an int
double_value has a value which is a float
bool_value has a value which is a KBaseSearchEngine.boolean
min_int has a value which is an int
max_int has a value which is an int
min_date has a value which is an int
max_date has a value which is an int
min_double has a value which is a float
max_double has a value which is a float


=end text

=back



=head2 MatchFilter

=over 4



=item Description

Optional rules of defining constrains for object properties
including values of keywords or metadata/system properties (like
object name, creation time range) or full-text search in all
properties.

boolean exclude_subobjects - don't return any subobjects in the search results if true.
    Default false.
list<string> source_tags - source tags are arbitrary strings applied to data at the data
    source (for example, the workspace service). The source_tags list may optionally be
    populated with a set of tags that will determine what data is returned in a search.
    By default, the list behaves as a whitelist and only data with at least one of the
    tags will be returned.
source_tags_blacklist - if true, the source_tags list behaves as a blacklist and any
    data with at least one of the tags will be excluded from the search results. If missing
    or false, the default behavior is maintained.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
full_text_in_all has a value which is a string
object_name has a value which is a string
timestamp has a value which is a KBaseSearchEngine.MatchValue
exclude_subobjects has a value which is a KBaseSearchEngine.boolean
lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
source_tags has a value which is a reference to a list where each element is a string
source_tags_blacklist has a value which is a KBaseSearchEngine.boolean

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
full_text_in_all has a value which is a string
object_name has a value which is a string
timestamp has a value which is a KBaseSearchEngine.MatchValue
exclude_subobjects has a value which is a KBaseSearchEngine.boolean
lookup_in_keys has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.MatchValue
source_tags has a value which is a reference to a list where each element is a string
source_tags_blacklist has a value which is a KBaseSearchEngine.boolean


=end text

=back



=head2 AccessFilter

=over 4



=item Description

Optional rules of access constraints.
  - with_private - include data found in workspaces not marked 
      as public, default value is true,
  - with_public - include data found in public workspaces,
      default value is false,
  - with_all_history - include all versions (last one and all
      old versions) of objects matching constrains, default
      value is false.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
with_private has a value which is a KBaseSearchEngine.boolean
with_public has a value which is a KBaseSearchEngine.boolean
with_all_history has a value which is a KBaseSearchEngine.boolean

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
with_private has a value which is a KBaseSearchEngine.boolean
with_public has a value which is a KBaseSearchEngine.boolean
with_all_history has a value which is a KBaseSearchEngine.boolean


=end text

=back



=head2 SearchTypesInput

=over 4



=item Description

Input parameters for search_types method.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
match_filter has a value which is a KBaseSearchEngine.MatchFilter
access_filter has a value which is a KBaseSearchEngine.AccessFilter

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
match_filter has a value which is a KBaseSearchEngine.MatchFilter
access_filter has a value which is a KBaseSearchEngine.AccessFilter


=end text

=back



=head2 SearchTypesOutput

=over 4



=item Description

Output results of search_types method.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
type_to_count has a value which is a reference to a hash where the key is a string and the value is an int
search_time has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
type_to_count has a value which is a reference to a hash where the key is a string and the value is an int
search_time has a value which is an int


=end text

=back



=head2 SortingRule

=over 4



=item Description

Rule for sorting results. 

string property - the property to sort on. This may be a an object property - e.g. a 
    field inside the object - or a standard property possessed by all objects, like a
    timestamp or creator.
boolean is_object_property - true (the default) to specify an object property, false to
    specify a standard property.
boolean ascending - true (the default) to sort ascending, false to sort descending.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
property has a value which is a string
is_object_property has a value which is a KBaseSearchEngine.boolean
ascending has a value which is a KBaseSearchEngine.boolean

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
property has a value which is a string
is_object_property has a value which is a KBaseSearchEngine.boolean
ascending has a value which is a KBaseSearchEngine.boolean


=end text

=back



=head2 Pagination

=over 4



=item Description

Pagination rules. Default values are: start = 0, count = 50.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
start has a value which is an int
count has a value which is an int

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
start has a value which is an int
count has a value which is an int


=end text

=back



=head2 PostProcessing

=over 4



=item Description

Rules for what to return about found objects.
skip_keys - do not include keyword values for object 
    ('key_props' field in ObjectData structure),
skip_data - do not include raw data for object ('data' and 
    'parent_data' fields in ObjectData structure),
include_highlight - include highlights of fields that
     matched query,
ids_only - shortcut to mark both skips as true and 
     include_highlight as false.
add_narrative_info - if true, narrative info gets added to the
     search results. Default is false.
add_access_group_info - if true, access groups and objects info get added
     to the search results. Default is false.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
ids_only has a value which is a KBaseSearchEngine.boolean
skip_keys has a value which is a KBaseSearchEngine.boolean
skip_data has a value which is a KBaseSearchEngine.boolean
include_highlight has a value which is a KBaseSearchEngine.boolean
add_narrative_info has a value which is a KBaseSearchEngine.boolean
add_access_group_info has a value which is a KBaseSearchEngine.boolean

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
ids_only has a value which is a KBaseSearchEngine.boolean
skip_keys has a value which is a KBaseSearchEngine.boolean
skip_data has a value which is a KBaseSearchEngine.boolean
include_highlight has a value which is a KBaseSearchEngine.boolean
add_narrative_info has a value which is a KBaseSearchEngine.boolean
add_access_group_info has a value which is a KBaseSearchEngine.boolean


=end text

=back



=head2 SearchObjectsInput

=over 4



=item Description

Input parameters for 'search_objects' method.
object_types - list of the types of objects to search on (optional). The
               function will search on all objects if the list is not specified
               or is empty. The list size must be less than 50.
match_filter - see MatchFilter.
sorting_rules - see SortingRule (optional).
access_filter - see AccessFilter.
pagination - see Pagination (optional).
post_processing - see PostProcessing (optional).


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
object_types has a value which is a reference to a list where each element is a string
match_filter has a value which is a KBaseSearchEngine.MatchFilter
sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
access_filter has a value which is a KBaseSearchEngine.AccessFilter
pagination has a value which is a KBaseSearchEngine.Pagination
post_processing has a value which is a KBaseSearchEngine.PostProcessing

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
object_types has a value which is a reference to a list where each element is a string
match_filter has a value which is a KBaseSearchEngine.MatchFilter
sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
access_filter has a value which is a KBaseSearchEngine.AccessFilter
pagination has a value which is a KBaseSearchEngine.Pagination
post_processing has a value which is a KBaseSearchEngine.PostProcessing


=end text

=back



=head2 ObjectData

=over 4



=item Description

Properties of an object including metadata, raw data and keywords.
GUID guid - the object's guid.
GUID parent_guid - the guid of the object's parent if the object is a subobject (e.g.
    features for genomes).
object_name - the object's name.
timestamp - the creation date for the object in milliseconds since the epoch.
string type - the type of the data in the search index.
int type_ver - the version of the search type.
string creator - the username of the user that created that data.
string copier - if this instance of the data is a copy, the username of the user that
    copied the data.
string mod - the name of the KBase SDK module that was used to create the data.
string method - the name of the method in the KBase SDK module that was used to create the
    data.
string module_ver - the version of the KBase SDK module that was used to create the data.
string commit - the version control commit hash of the KBase SDK module that was used to
    create the data. 
parent_data - raw data extracted from the subobject's parent object. The data contents will
    vary from object to object. Null if the object is not a subobject.
data - raw data extracted from the object. The data contents will vary from object to object.
key_props - keyword properties of the object. These fields have been extracted from the object
   and possibly transformed based on the search specification for the object.
   The contents will vary from object to object.
mapping<string, list<string>> highlight - The keys are the field names and the list 
    contains the sections in each field that matched the search query. Fields with no
    hits will not be available. Short fields that matched are shown in their entirety.
    Longer fields are shown as snippets preceded or followed by "...".


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
guid has a value which is a KBaseSearchEngine.GUID
parent_guid has a value which is a KBaseSearchEngine.GUID
object_name has a value which is a string
timestamp has a value which is an int
type has a value which is a string
type_ver has a value which is an int
creator has a value which is a string
copier has a value which is a string
mod has a value which is a string
method has a value which is a string
module_ver has a value which is a string
commit has a value which is a string
parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
data has a value which is an UnspecifiedObject, which can hold any non-null object
key_props has a value which is a reference to a hash where the key is a string and the value is a string
highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
guid has a value which is a KBaseSearchEngine.GUID
parent_guid has a value which is a KBaseSearchEngine.GUID
object_name has a value which is a string
timestamp has a value which is an int
type has a value which is a string
type_ver has a value which is an int
creator has a value which is a string
copier has a value which is a string
mod has a value which is a string
method has a value which is a string
module_ver has a value which is a string
commit has a value which is a string
parent_data has a value which is an UnspecifiedObject, which can hold any non-null object
data has a value which is an UnspecifiedObject, which can hold any non-null object
key_props has a value which is a reference to a hash where the key is a string and the value is a string
highlight has a value which is a reference to a hash where the key is a string and the value is a reference to a list where each element is a string


=end text

=back



=head2 access_group_id

=over 4



=item Description

A data source access group ID (for instance, the integer ID of a workspace).


=item Definition

=begin html

<pre>
an int
</pre>

=end html

=begin text

an int

=end text

=back



=head2 timestamp

=over 4



=item Description

A timestamp in milliseconds since the epoch.


=item Definition

=begin html

<pre>
an int
</pre>

=end html

=begin text

an int

=end text

=back



=head2 narrative_info

=over 4



=item Description

Information about a workspace, which may or may not contain a KBase Narrative.
This data is specific for data from the Workspace Service.

string narrative_name - the name of the narrative contained in the workspace, or null if
    the workspace does not contain a narrative.
int narrative_id - the id of the narrative contained in the workspace, or null.
timestamp time_last_saved - the modification date of the workspace.
string ws_owner_username - the unique user name of the workspace's owner.
string ws_owner_displayname - the display name of the workspace's owner.


=item Definition

=begin html

<pre>
a reference to a list containing 5 items:
0: (narrative_name) a string
1: (narrative_id) an int
2: (time_last_saved) a KBaseSearchEngine.timestamp
3: (ws_owner_username) a string
4: (ws_owner_displayname) a string

</pre>

=end html

=begin text

a reference to a list containing 5 items:
0: (narrative_name) a string
1: (narrative_id) an int
2: (time_last_saved) a KBaseSearchEngine.timestamp
3: (ws_owner_username) a string
4: (ws_owner_displayname) a string


=end text

=back



=head2 access_group_info

=over 4



=item Description

The access_group_info and object_info are meant to be abstractions for info from multiple data sources.
Until other data sources become available, definitions pertaining to Workspace are being used.
When other data sources are available, the following variables will be moved from
this concrete workspace definitions, to structures with higher level abstractions.


=item Definition

=begin html

<pre>
a Workspace.workspace_info
</pre>

=end html

=begin text

a Workspace.workspace_info

=end text

=back



=head2 object_info

=over 4



=item Definition

=begin html

<pre>
a Workspace.object_info
</pre>

=end html

=begin text

a Workspace.object_info

=end text

=back



=head2 SearchObjectsOutput

=over 4



=item Description

Output results for 'search_objects' method.
'pagination' and 'sorting_rules' fields show actual input for
    pagination and sorting.
total - total number of found objects.
search_time - common time in milliseconds spent.
mapping<access_group_id, narrative_info> access_group_narrative_info - information about
   the workspaces in which the objects in the results reside. This data only applies to
   workspace objects.
mapping<access_group_id, access_group_info> access_groups_info - information about
   the access groups in which the objects in the results reside. Currently this data only applies to
   workspace objects. The tuple9 value returned by get_workspace_info() for each workspace
   in the search results is saved in this mapping. In future the access_group_info will be
   replaced with a higher level abstraction.
mapping<obj_ref, object_info> objects_info - information about each object in the
   search results. Currently this data only applies to workspace objects. The tuple11 value
   returned by get_object_info3() for each object in the search results is saved in the mapping.
   In future the object_info will be replaced with a higher level abstraction.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
pagination has a value which is a KBaseSearchEngine.Pagination
sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
total has a value which is an int
search_time has a value which is an int
access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a KBaseSearchEngine.object_info

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
pagination has a value which is a KBaseSearchEngine.Pagination
sorting_rules has a value which is a reference to a list where each element is a KBaseSearchEngine.SortingRule
objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
total has a value which is an int
search_time has a value which is an int
access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a KBaseSearchEngine.object_info


=end text

=back



=head2 GetObjectsInput

=over 4



=item Description

Input parameters for get_objects method.
    guids - list of guids
    post_processing - see PostProcessing (optional).
    match_filter - see MatchFilter (optional).


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
guids has a value which is a reference to a list where each element is a KBaseSearchEngine.GUID
post_processing has a value which is a KBaseSearchEngine.PostProcessing
match_filter has a value which is a KBaseSearchEngine.MatchFilter

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
guids has a value which is a reference to a list where each element is a KBaseSearchEngine.GUID
post_processing has a value which is a KBaseSearchEngine.PostProcessing
match_filter has a value which is a KBaseSearchEngine.MatchFilter


=end text

=back



=head2 GetObjectsOutput

=over 4



=item Description

Output results of get_objects method.

mapping<access_group_id, narrative_info> access_group_narrative_info - information about
   the workspaces in which the objects in the results reside. This data only applies to
   workspace objects.
mapping<access_group_id, access_group_info> access_groups_info - information about
   the access groups in which the objects in the results reside. Currently this data only applies to
   workspace objects. The tuple9 value returned by get_workspace_info() for each workspace
   in the search results is saved in this mapping. In future the access_group_info will be
   replaced with a higher level abstraction.
mapping<obj_ref, object_info> objects_info - information about each object in the
   search results. Currently this data only applies to workspace objects. The tuple11 value
   returned by get_object_info3() for each object in the search results is saved in the mapping.
   In future the object_info will be replaced with a higher level abstraction.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
search_time has a value which is an int
access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a Workspace.object_info

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
objects has a value which is a reference to a list where each element is a KBaseSearchEngine.ObjectData
search_time has a value which is an int
access_group_narrative_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.narrative_info
access_groups_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.access_group_id and the value is a KBaseSearchEngine.access_group_info
objects_info has a value which is a reference to a hash where the key is a KBaseSearchEngine.obj_ref and the value is a Workspace.object_info


=end text

=back



=head2 ListTypesInput

=over 4



=item Description

Input parameters for list_types method.
type_name - optional parameter; if not specified all types are described.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
type_name has a value which is a string

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
type_name has a value which is a string


=end text

=back



=head2 KeyDescription

=over 4



=item Description

Description of searchable type keyword. 
    - key_value_type can be one of {'string', 'integer', 'double', 
      'boolean'},
    - hidden - if true then this keyword provides values for other
      keywords (like in 'link_key') and is not supposed to be shown.
    - link_key - optional field pointing to another keyword (which is
      often hidden) providing GUID to build external URL to.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
key_name has a value which is a string
key_ui_title has a value which is a string
key_value_type has a value which is a string
hidden has a value which is a KBaseSearchEngine.boolean
link_key has a value which is a string

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
key_name has a value which is a string
key_ui_title has a value which is a string
key_value_type has a value which is a string
hidden has a value which is a KBaseSearchEngine.boolean
link_key has a value which is a string


=end text

=back



=head2 TypeDescriptor

=over 4



=item Description

Description of searchable object type including details about keywords.
TODO: add more details like parent type, primary key, ...


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
type_name has a value which is a string
type_ui_title has a value which is a string
keys has a value which is a reference to a list where each element is a KBaseSearchEngine.KeyDescription

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
type_name has a value which is a string
type_ui_title has a value which is a string
keys has a value which is a reference to a list where each element is a KBaseSearchEngine.KeyDescription


=end text

=back



=head2 ListTypesOutput

=over 4



=item Description

Output results of list_types method.


=item Definition

=begin html

<pre>
a reference to a hash where the following keys are defined:
types has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.TypeDescriptor

</pre>

=end html

=begin text

a reference to a hash where the following keys are defined:
types has a value which is a reference to a hash where the key is a string and the value is a KBaseSearchEngine.TypeDescriptor


=end text

=back



=cut

package KBaseSearchEngine::KBaseSearchEngineClient::RpcClient;
use base 'JSON::RPC::Client';
use POSIX;
use strict;

#
# Override JSON::RPC::Client::call because it doesn't handle error returns properly.
#

sub call {
    my ($self, $uri, $headers, $obj) = @_;
    my $result;


    {
	if ($uri =~ /\?/) {
	    $result = $self->_get($uri);
	}
	else {
	    Carp::croak "not hashref." unless (ref $obj eq 'HASH');
	    $result = $self->_post($uri, $headers, $obj);
	}

    }

    my $service = $obj->{method} =~ /^system\./ if ( $obj );

    $self->status_line($result->status_line);

    if ($result->is_success) {

        return unless($result->content); # notification?

        if ($service) {
            return JSON::RPC::ServiceObject->new($result, $self->json);
        }

        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    elsif ($result->content_type eq 'application/json')
    {
        return JSON::RPC::ReturnObject->new($result, $self->json);
    }
    else {
        return;
    }
}


sub _post {
    my ($self, $uri, $headers, $obj) = @_;
    my $json = $self->json;

    $obj->{version} ||= $self->{version} || '1.1';

    if ($obj->{version} eq '1.0') {
        delete $obj->{version};
        if (exists $obj->{id}) {
            $self->id($obj->{id}) if ($obj->{id}); # if undef, it is notification.
        }
        else {
            $obj->{id} = $self->id || ($self->id('JSON::RPC::Client'));
        }
    }
    else {
        # $obj->{id} = $self->id if (defined $self->id);
	# Assign a random number to the id if one hasn't been set
	$obj->{id} = (defined $self->id) ? $self->id : substr(rand(),2);
    }

    my $content = $json->encode($obj);

    $self->ua->post(
        $uri,
        Content_Type   => $self->{content_type},
        Content        => $content,
        Accept         => 'application/json',
	@$headers,
	($self->{token} ? (Authorization => $self->{token}) : ()),
    );
}



1;
