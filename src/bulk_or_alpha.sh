#!/bin/bash

SCHEMA="./graphQL/main.schema"
RDFFILE="./data/data.rdf.gz"

DIR="./out/0"
my_alpha_p_0="${DIR}/p"

check_existing_dir () {
    if [ ! -d "${DIR}" ]; then
        echo "Directory ${DIR} from Bulk - not found!"
        return 1
    else
        return 0
    fi
}

check_existing_Schema () {
    if [ ! -f "${SCHEMA}" ]; then
        echo "Schema not found!"
        return 1
    else
        cat $SCHEMA
        return 0
    fi
}

check_existing_RDF () {
    if [ ! -f "${RDFFILE}" ]; then
        echo "RDF not found!"
        return 1
    else
        echo "================= We have an RDF file =================="
        return 0
    fi
}

RUN_alpha () {
    echo "Dgraph Alpha Starting ..."
    dgraph alpha --bindall=true --my=alpha:7080 --zero=zero:5080 -p ${my_alpha_p_0} --security "whitelist=0.0.0.0/0" --cache "percentage=0,65,35"
}

RUN_BulkLoader () {
    if check_existing_RDF && check_existing_Schema; then
        echo "Dgraph BulkLoader Starting..."
        dgraph bulk -f ${RDFFILE} -s ${SCHEMA} --reduce_shards=1 --zero=zero:5080
        return 0
    else
        echo "You need to provide both an RDF and a Schema file"
        return 1
    fi
}

if check_existing_dir; then
    RUN_alpha
else
    if RUN_BulkLoader; then
        RUN_alpha
    fi
fi

exit
