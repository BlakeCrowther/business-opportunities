'use client';

import type { QueryResponse } from '@/types/python';
import { useState } from 'react';

export default function QueryInterface() {
  const [query, setQuery] = useState('');
  const [context, setContext] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<QueryResponse['data'] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          additional_context: context,
        }),
      });

      const data: QueryResponse = await response.json();
      
      if (data.success && data.data) {
        setResult(data.data);
      } else {
        setError(data.error || 'An unknown error occurred');
      }
    } catch (err) {
      setError('Failed to process query');
      console.error('Error:', err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-4xl mx-auto p-6">
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="query" className="block text-sm font-medium text-gray-700">
            What would you like to know about the business landscape?
          </label>
          <textarea
            id="query"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            rows={3}
            required
          />
        </div>

        <div>
          <label htmlFor="context" className="block text-sm font-medium text-gray-700">
            Additional Context (optional)
          </label>
          <textarea
            id="context"
            value={context}
            onChange={(e) => setContext(e.target.value)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            rows={2}
          />
        </div>

        <button
          type="submit"
          disabled={loading}
          className="inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50"
        >
          {loading ? 'Processing...' : 'Submit Query'}
        </button>
      </form>

      {error && (
        <div className="mt-4 p-4 bg-red-50 text-red-700 rounded-md">
          {error}
        </div>
      )}

      {result && (
        <div className="mt-6 space-y-4">
          <div className="bg-gray-50 p-4 rounded-md">
            <h3 className="font-medium">Query Interpretation</h3>
            <p className="mt-2">{result.query}</p>
          </div>

          <div className="bg-gray-50 p-4 rounded-md">
            <h3 className="font-medium">Reasoning</h3>
            <p className="mt-2 whitespace-pre-wrap">{result.reasoning}</p>
          </div>

          <div className="bg-gray-50 p-4 rounded-md">
            <h3 className="font-medium">Results</h3>
            <p className="mt-2 whitespace-pre-wrap">{result.interpretation}</p>
          </div>

          {result.suggested_queries && result.suggested_queries.length > 0 && (
            <div className="bg-gray-50 p-4 rounded-md">
              <h3 className="font-medium">Suggested Follow-up Questions</h3>
              <ul className="mt-2 list-disc list-inside">
                {result.suggested_queries.map((q, i) => (
                  <li key={i} className="cursor-pointer text-blue-600 hover:text-blue-800"
                      onClick={() => setQuery(q)}>
                    {q}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
} 