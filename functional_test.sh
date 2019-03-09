./build.sh

mkdir functional_test_results/
rm -rf functional_test_results/storage
rm -rf functional_test_results/checkpoint
mkdir functional_test_results/checkpoint

cat <<EOF > functional_test_results/config.json
{
    "planner": {
        "checkPointLevel": "IntermediateResult",
        "checkPointRoot": "$PWD/functional_test_results/checkpoint"
    },
    "journal": {
        "path": "$PWD/functional_test_results/journal.db"
    },
    "storage": {
        "className":"ro.yota.weave.storage.FSStorage",
        "rootPath":"$PWD/functional_test_results/storage"
    },
    "publishers": [
        {
            "className": "ro.yota.weave.FSPublisher"
        }
    ],
    "scheduler": {
        "className":"ro.yota.weave.AkkaScheduler",
        "scriptEnvVars": [
            ["PATH", "$PWD/bin:$PATH"]
        ]
    }
}
EOF

testObjects=(
    core.Test.result
)

for testObject in $testObjects
do
    ./bin/weave -c functional_test_results/config.json -j ./sample/target/scala-2.11/sampleRecipe-assembly-0.1-SNAPSHOT.jar --debug order ro.yota.weave.functional_test.$testObject
done

rm -rf functional_test_results/storage

# Check checkpoint imports
./bin/weave -c functional_test_results/config.json -j ./sample/target/scala-2.11/sampleRecipe-assembly-0.1-SNAPSHOT.jar --debug order ro.yota.weave.functional_test.core.Test.result


failed=$(grep "FAILED" $PWD/functional_test_results/checkpoint/*.assertions.txt | wc -l)
success=$(grep "SUCCESS" $PWD/functional_test_results/checkpoint/*.assertions.txt | wc -l)

if [ ${failed} -ne 0 ] ; then
    echo "${failed} test(s) failed. (${success} test(s) succeeded)"
    exit 1
else
    echo "${success} tests succeeded"
    exit 0
fi
