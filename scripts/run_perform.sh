script_dir=$(dirname "$(readlink -f "$0")")
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
$script_dir/../bin/run_KBaseRelationEngine_perform.sh
