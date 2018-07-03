script_dir=$(dirname "$(readlink -f "$0")")
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
$script_dir/../bin/run_KBaseSearchEngine_perform.sh | tee $script_dir/../work/perform.txt
