#!/bin/bash
# run_threshold_tests.sh

echo "================================"
echo "EXECUTANDO 5 TESTES - KAFKA"
echo "================================"

for i in {1..5}
do
    echo ""
    echo ">>> TESTE $i/5 - KAFKA <<<"
    python kafka_threshold_auto.py
    if [ $? -ne 0 ]; then
        echo "⚠️  Erro no teste $i, continuando..."
    fi
    echo "Aguardando 10 segundos..."
    sleep 10
done

echo ""
echo "================================"
echo "EXECUTANDO 5 TESTES - RABBITMQ"
echo "================================"

for i in {1..5}
do
    echo ""
    echo ">>> TESTE $i/5 - RABBITMQ <<<"
    python rabbitmq_threshold_auto.py
    if [ $? -ne 0 ]; then
        echo "⚠️  Erro no teste $i, continuando..."
    fi
    echo "Aguardando 10 segundos..."
    sleep 10
done

echo ""
echo "✅ TODOS OS TESTES CONCLUÍDOS!"
echo "Arquivos gerados:"
ls -lh *threshold_test_*.txt 2>/dev/null || echo "Nenhum arquivo gerado"