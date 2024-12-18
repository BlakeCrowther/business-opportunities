export interface PythonResponse {
  success: boolean;
  data?: any;
  error?: string;
}

export interface ProcessingRequest {
  input: string;
  options?: Record<string, any>;
}

export interface QueryRequest {
  query: string;
  additional_context?: string;
}

export interface QueryResponse {
  success: boolean;
  data?: {
    query: string;
    reasoning: string;
    interpretation: string;
    suggested_queries: string[];
  };
  error?: string;
} 