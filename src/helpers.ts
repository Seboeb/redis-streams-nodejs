export const timeout = async (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export type PropType<T, K extends keyof T> = T[K];
