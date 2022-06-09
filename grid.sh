set -x

cargo build --release

for dataset in hdfs-logs gh-archive wikipedia nginx-logs
do
    for algo in lz4 zstd
    do
        for blocksize in 16 32 64 96 128 192 256 512 1024 2048
        do
            index_id="$dataset-$algo-$blocksize"

            if grep -q "$dataset,.*,$algo,$blocksize" grid.csv
            then
                echo "Skipping $index_id"
                continue
            fi

            target/release/quickwit index create --index-config "config/tutorials/$dataset/index-config.yaml" --index-id $index_id --docstore-blocksize $((blocksize * 1024)) --docstore-compression $algo

            start=$(date +%s)

            case $dataset in
                hdfs-logs)
                    [ ! -e hdfs.logs.quickwit.json.gz ] && wget https://quickwit-datasets-public.s3.amazonaws.com/hdfs.logs.quickwit.json.gz
                    gunzip -c hdfs.logs.quickwit.json.gz | target/release/quickwit index ingest --index $index_id
                ;;
                gh-archive)
                    [ ! -e 2022-05-12-10.json.gz ] && wget https://data.gharchive.org/2022-05-12-{10..15}.json.gz
                    gunzip -c 2022-*.json.gz | jq -c '.created_at = (.created_at | fromdate) | .public = if .public then 1 else 0 end' | target/release/quickwit index ingest --index $index_id
                ;;
                wikipedia)
                    [ ! -e wiki-articles.json.tar.gz ] && wget https://quickwit-datasets-public.s3.amazonaws.com/wiki-articles.json.tar.gz
                    tar -xOzf wiki-articles.json.tar.gz | target/release/quickwit index ingest --index $index_id
                ;;
                nginx-logs)
                    [ ! -e nginx-logs.json.gz ] && wget https://quickwit-datasets-public.s3.amazonaws.com/nginx-logs.json.gz
                    gunzip -c nginx-logs.json.gz | jq -c '.datetime = (.datetime | fromdate)' | target/release/quickwit index ingest --index $index_id
                ;;
            esac

            end=$(date +%s)
            runtime=$((end - start))

            num_docs=$(target/release/quickwit index describe --index $index_id | grep 'Number of published documents' | cut -d':' -f2 | tr -d ' ')
            num_splits=$(target/release/quickwit index describe --index $index_id | grep 'Number of published splits' | cut -d':' -f2 | tr -d ' ')

            split_id=$(target/release/quickwit split list --index $index_id | grep Published | cut -d'|' -f2 | tr -d ' ')
            size=$(target/release/quickwit split list --index $index_id | grep Published | cut -d'|' -f5 | tr -d ' ')
            storesize=$(target/release/quickwit split describe --index $index_id --split $split_id | grep .store | cut -d' ' -f2)

            echo "$dataset,$size,$num_docs,$num_splits,$algo,$blocksize,$storesize,$runtime" >> grid.csv

            target/release/quickwit index delete --index $index_id
        done
    done
done
