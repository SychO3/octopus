import { apiClient } from '@/api/client';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

export type ModelMapping = {
    id: number;
    request_name: string;
    actual_name: string;
    enabled: boolean;
};

export function useModelMappingList() {
    return useQuery({
        queryKey: ['model-mapping', 'list'],
        queryFn: async () => {
            const res = await apiClient.get<{ data: ModelMapping[] }>('/api/v1/model-mapping/list');
            return res.data.data ?? [];
        },
    });
}

export function useCreateModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (data: { request_name: string; actual_name: string; enabled?: boolean }) => {
            const res = await apiClient.post('/api/v1/model-mapping/create', data);
            return res.data;
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}

export function useUpdateModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (data: { id: number; request_name: string; actual_name: string; enabled?: boolean }) => {
            const res = await apiClient.post('/api/v1/model-mapping/update', data);
            return res.data;
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}

export function useDeleteModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (id: number) => {
            const res = await apiClient.delete(`/api/v1/model-mapping/delete/${id}`);
            return res.data;
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}
