#!/bin/bash

# Test all gpt-5.5 channels with both endpoints
declare -A CHANNELS
CHANNELS[7]="薄荷/level1|https://x666.me/v1|sk-1IHMPN8NyK6rOYac7tYMEizpijlcFdbWAK0nwNuNqbbIUvJC|gpt-5.5-nx"
CHANNELS[17]="CHY|https://chybenzun.top/v1|sk-ANNqkRHD5Unk8yTyLR6I0pC0utxPt1DFS4TayoQ2P3mi2n5y|gpt-5.5"
CHANNELS[27]="魔方|https://www.mofas.one/v1|sk-LQzcK96vjc4WCUQKnzKjVrYVq95VXxPiNob46ZdgOgpukgWv|gpt-5.5"
CHANNELS[31]="iMeagic|https://newapi.imagic.eu.org/v1|sk-EobvjMaXgY3KMZ6hr9GSDxr0gqrlObcaFfeDurIa7vbmkDDO|gpt-5.5"
CHANNELS[33]="Gundam|https://ai.gunddam.dpdns.org/v1|sk-22606363a13e72dedf5bc67b60b6e79d339a1370bd12e4fdc2a35c26a44183e4|gpt-5.5"
CHANNELS[39]="薄荷/level2|https://x666.me/v1|sk-GQJbHnDKtGz5mynqyFoBCrbwDWCKO4LpNLLYNpmoybNHQkgV|gpt-5.5-nx"
CHANNELS[52]="越南佬|https://api.xpiki.com/v1|sk-RibBcLmMrcaqMXSDTNjvyJqKCKdnphpA1NxDIW2HS74|gpt-5.5"
CHANNELS[67]="liWAN|https://metapi.lilililwan.xyz/v1|sk-MAvonPnVaNL0UplxOt6g9zLFOXU84QEutGqS0MXLNF8G9tXQ|gpt-5.5"
CHANNELS[72]="LY Free|https://free.lyclaude.site/v1|sk-vrWpEQpihphKg09E2qHXH4Gqzq4LfuT4XNZ2BHcf3lskBqOy|gpt-5.5"
CHANNELS[75]="自建codex|https://codex.515111.xyz/v1|sk-f53698266c30c90ab1aefa59c5189264d9d31a981340c9b8|gpt-5.5"
CHANNELS[76]="Ciallo|https://ioll.pp.ua/v1|sk-uXfjAYJJmjDPBd1ycNjC3lHczJunmXyh7oy0jcfT7FvgT8cr|gpt-5.5"
CHANNELS[84]="午夜API/default|https://api.lyjxka.top/v1|sk-9TaVNIcPM0biUxAu0uLe7WaQhbvQoaHMe8UizNNbfTJFW8qr|gpt-5.5"
CHANNELS[85]="午夜API/vip|https://api.lyjxka.top/v1|sk-zge94ylLekBoENFZnzfQ3XXMdKHky50PXfNN9UxG6KfCREN3|gpt-5.5"

CHAT_BODY='{"model":"%s","messages":[{"role":"user","content":"hi"}],"max_tokens":5,"stream":false}'
RESP_BODY='{"model":"%s","input":"hi","max_output_tokens":5,"stream":false}'

echo "==============================================="
echo " GPT-5.5 全渠道端点测试"
echo " 时间: $(date)"
echo "==============================================="
echo ""

for id in $(echo "${!CHANNELS[@]}" | tr ' ' '\n' | sort -n); do
    IFS='|' read -r name base_url key model <<< "${CHANNELS[$id]}"
    echo "--- Channel $id: $name (model: $model) ---"

    # Test /chat/completions
    body=$(printf "$CHAT_BODY" "$model")
    result=$(curl -s --max-time 30 "${base_url}/chat/completions" \
        -H "Authorization: Bearer $key" \
        -H "Content-Type: application/json" \
        -d "$body" 2>&1)

    content=$(echo "$result" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    if 'error' in d:
        print(f'ERROR: {d[\"error\"].get(\"message\",d[\"error\"])}')
    elif 'choices' in d and len(d['choices'])>0:
        c=d['choices'][0].get('message',{}).get('content','')
        if c: print(f'OK: \"{c[:60]}\"')
        else: print('EMPTY: choices存在但content为空')
    else:
        print(f'UNEXPECTED: {str(d)[:100]}')
except: print(f'PARSE_ERROR: {sys.stdin.read()[:100] if False else repr(result)[:100]}')
" 2>&1 <<< "$result")
    echo "  /chat/completions: $content"

    # Test /responses
    body=$(printf "$RESP_BODY" "$model")
    result=$(curl -s --max-time 30 "${base_url}/responses" \
        -H "Authorization: Bearer $key" \
        -H "Content-Type: application/json" \
        -d "$body" 2>&1)

    content=$(echo "$result" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    if 'error' in d:
        print(f'ERROR: {d[\"error\"].get(\"message\",d[\"error\"])}')
    elif 'output' in d:
        items=d.get('output',[])
        text=''
        for item in items:
            if item.get('type')=='message':
                for c in item.get('content',[]):
                    text+=c.get('text','')
        if text: print(f'OK: \"{text[:60]}\"')
        else: print('EMPTY: output存在但无text')
    elif d.get('id',''):
        print(f'OK(has id): {str(d)[:80]}')
    else:
        print(f'UNEXPECTED: {str(d)[:100]}')
except Exception as e: print(f'PARSE_ERROR: {str(e)[:50]}')
" 2>&1 <<< "$result")
    echo "  /responses:        $content"
    echo ""
done
