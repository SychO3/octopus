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
            return apiClient.get<ModelMapping[]>('/api/v1/model-mapping/list');
        },
    });
}

export function useCreateModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (data: { request_name: string; actual_name: string; enabled?: boolean }) => {
            return apiClient.post<ModelMapping>('/api/v1/model-mapping/create', data);
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}

export function useUpdateModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (data: { id: number; request_name: string; actual_name: string; enabled?: boolean }) => {
            return apiClient.post<ModelMapping>('/api/v1/model-mapping/update', data);
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}

export function useDeleteModelMapping() {
    const queryClient = useQueryClient();
    return useMutation({
        mutationFn: async (id: number) => {
            return apiClient.delete<null>(`/api/v1/model-mapping/delete/${id}`);
        },
        onSuccess: () => queryClient.invalidateQueries({ queryKey: ['model-mapping'] }),
    });
}
