'use client';

import { useState } from 'react';
import { ArrowRight, Plus, Trash2, Replace } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { toast } from '@/components/common/Toast';
import { SettingCard } from './shared';
import {
    useModelMappingList,
    useCreateModelMapping,
    useUpdateModelMapping,
    useDeleteModelMapping,
    type ModelMapping,
} from '@/api/endpoints/model-mapping';

export function SettingModelMapping() {
    const { data: mappings = [], isLoading } = useModelMappingList();
    const createMutation = useCreateModelMapping();
    const updateMutation = useUpdateModelMapping();
    const deleteMutation = useDeleteModelMapping();

    const [newRequestName, setNewRequestName] = useState('');
    const [newActualName, setNewActualName] = useState('');

    const handleCreate = () => {
        const req = newRequestName.trim();
        const act = newActualName.trim();
        if (!req || !act) {
            toast.error('请填写请求模型名和实际模型名');
            return;
        }
        createMutation.mutate(
            { request_name: req, actual_name: act },
            {
                onSuccess: () => {
                    setNewRequestName('');
                    setNewActualName('');
                    toast.success('映射已添加');
                },
                onError: (err) => toast.error(err instanceof Error ? err.message : '添加失败'),
            },
        );
    };

    const handleToggle = (mapping: ModelMapping) => {
        updateMutation.mutate({
            id: mapping.id,
            request_name: mapping.request_name,
            actual_name: mapping.actual_name,
            enabled: !mapping.enabled,
        });
    };

    const handleDelete = (id: number) => {
        deleteMutation.mutate(id, {
            onSuccess: () => toast.success('映射已删除'),
            onError: (err) => toast.error(err instanceof Error ? err.message : '删除失败'),
        });
    };

    return (
        <SettingCard icon={Replace} title="模型映射">
            <div className="space-y-3">
                {isLoading ? (
                    <div className="text-sm text-muted-foreground">加载中...</div>
                ) : mappings.length === 0 ? (
                    <div className="text-sm text-muted-foreground">暂无映射规则</div>
                ) : (
                    mappings.map((mapping) => (
                        <div key={mapping.id} className="flex items-center gap-2">
                            <Switch
                                checked={mapping.enabled}
                                onCheckedChange={() => handleToggle(mapping)}
                                className="shrink-0"
                            />
                            <span className="min-w-0 flex-1 truncate text-sm font-mono">{mapping.request_name}</span>
                            <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />
                            <span className="min-w-0 flex-1 truncate text-sm font-mono">{mapping.actual_name}</span>
                            <Button
                                variant="ghost"
                                size="icon"
                                className="size-7 shrink-0 text-muted-foreground hover:text-destructive"
                                onClick={() => handleDelete(mapping.id)}
                                disabled={deleteMutation.isPending}
                            >
                                <Trash2 className="size-3.5" />
                            </Button>
                        </div>
                    ))
                )}

                <div className="flex items-center gap-2 pt-2 border-t">
                    <Input
                        value={newRequestName}
                        onChange={(e) => setNewRequestName(e.target.value)}
                        placeholder="请求模型名"
                        className="flex-1 rounded-xl text-sm"
                    />
                    <ArrowRight className="size-3.5 shrink-0 text-muted-foreground" />
                    <Input
                        value={newActualName}
                        onChange={(e) => setNewActualName(e.target.value)}
                        placeholder="实际模型名"
                        className="flex-1 rounded-xl text-sm"
                        onKeyDown={(e) => e.key === 'Enter' && handleCreate()}
                    />
                    <Button
                        variant="outline"
                        size="icon"
                        className="size-8 shrink-0 rounded-xl"
                        onClick={handleCreate}
                        disabled={createMutation.isPending}
                    >
                        <Plus className="size-4" />
                    </Button>
                </div>
            </div>
        </SettingCard>
    );
}
