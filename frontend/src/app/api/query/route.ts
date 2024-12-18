import type { QueryRequest, QueryResponse } from '@/types/python';

import { NextResponse } from 'next/server';
import { PythonExecutor } from '@/lib/python/executor';

export async function POST(request: Request) {
  try {
    const body: QueryRequest = await request.json();
    const executor = new PythonExecutor();

    // Execute Python script with input
    const result = await executor.execute('main.py', [
      JSON.stringify(body)
    ]);

    return NextResponse.json({ 
      success: true, 
      data: JSON.parse(result) 
    } as QueryResponse);
  } catch (error) {
    console.error('Query execution error:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: 'Query processing failed' 
      } as QueryResponse,
      { status: 500 }
    );
  }
} 