'use client';

import { useTranslations } from 'next-intl';
import { Bot, RefreshCw, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { toast } from '@/components/common/Toast';
import { useSettingValue, useSetSetting, SettingKey } from '@/api/endpoints/setting';
import { apiClient } from '@/api/client';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useState, useEffect } from 'react';

interface ImpersonateVersions {
    claude: string;
    codex: string;
    gemini: string;
    updated_at: string;
}

function useImpersonateVersions() {
    return useQuery({
        queryKey: ['impersonate', 'versions'],
        queryFn: () => apiClient.get<ImpersonateVersions>('/api/v1/impersonate/versions'),
        refetchInterval: 60000,
    });
}

function useRefreshImpersonateVersions() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: () => apiClient.post<ImpersonateVersions>('/api/v1/impersonate/refresh'),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['impersonate', 'versions'] });
            queryClient.invalidateQueries({ queryKey: ['settings', 'list'] });
        },
    });
}

function VersionRow({ label, version, settingKey }: { label: string; version: string; settingKey: string }) {
    const { value: savedValue } = useSettingValue(settingKey, '');
    const setSetting = useSetSetting();
    const [customValue, setCustomValue] = useState('');
    const t = useTranslations('setting.impersonate');

    useEffect(() => {
        setCustomValue(savedValue);
    }, [savedValue]);

    const handleSave = () => {
        if (customValue !== savedValue) {
            setSetting.mutate({ key: settingKey, value: customValue });
        }
    };

    return (
        <div className="flex items-center justify-between gap-4">
            <div className="flex items-center gap-3 min-w-0">
                <span className="text-sm font-medium whitespace-nowrap">{label}</span>
                <code className="text-xs font-mono text-muted-foreground truncate">
                    {version || '—'}
                </code>
            </div>
            <Input
                value={customValue}
                onChange={(e) => setCustomValue(e.target.value)}
                onBlur={handleSave}
                onKeyDown={(e) => e.key === 'Enter' && handleSave()}
                placeholder={t('customPlaceholder')}
                className="w-36 h-8 text-xs rounded-lg"
            />
        </div>
    );
}

export function SettingImpersonate() {
    const t = useTranslations('setting.impersonate');
    const versionsQuery = useImpersonateVersions();
    const refreshMutation = useRefreshImpersonateVersions();

    const versions = versionsQuery.data;
    const updatedAt = versions?.updated_at
        ? new Date(versions.updated_at).toLocaleString()
        : t('neverUpdated');

    const handleRefresh = () => {
        refreshMutation.mutate(undefined, {
            onSuccess: () => toast.success(t('refreshSuccess')),
            onError: () => toast.error(t('refreshFailed')),
        });
    };

    return (
        <div className="rounded-3xl border border-border bg-card p-6 space-y-5">
            <div className="flex items-center justify-between">
                <h2 className="text-lg font-bold text-card-foreground flex items-center gap-2">
                    <Bot className="h-5 w-5" />
                    {t('title')}
                </h2>
                <Button
                    variant="ghost"
                    size="sm"
                    onClick={handleRefresh}
                    disabled={refreshMutation.isPending}
                    className="rounded-xl text-xs"
                >
                    {refreshMutation.isPending ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin mr-1" />
                    ) : (
                        <RefreshCw className="h-3.5 w-3.5 mr-1" />
                    )}
                    {refreshMutation.isPending ? t('refreshing') : t('refreshNow')}
                </Button>
            </div>

            <p className="text-xs text-muted-foreground">{t('description')}</p>

            <div className="space-y-3">
                <VersionRow
                    label={t('claude')}
                    version={versions?.claude || ''}
                    settingKey={SettingKey.CLIVersionsClaude}
                />
                <VersionRow
                    label={t('codex')}
                    version={versions?.codex || ''}
                    settingKey={SettingKey.CLIVersionsCodex}
                />
                <VersionRow
                    label={t('gemini')}
                    version={versions?.gemini || ''}
                    settingKey={SettingKey.CLIVersionsGemini}
                />
            </div>

            <div className="flex items-center justify-between pt-2 border-t border-border">
                <span className="text-xs text-muted-foreground">
                    {t('lastUpdated')}: {updatedAt}
                </span>
            </div>
        </div>
    );
}
