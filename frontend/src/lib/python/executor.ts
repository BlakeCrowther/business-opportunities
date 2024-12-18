import path from 'path';
import { spawn } from 'child_process';

export class PythonExecutor {
  private pythonPath: string;

  constructor() {
    // Adjust this path based on your Python environment
    this.pythonPath = process.env.PYTHON_PATH || 'python';
  }

  async execute(scriptName: string, args: string[] = []): Promise<string> {
    return new Promise((resolve, reject) => {
      // Note: Adjusted to look for scripts in the root directory
      const scriptPath = path.join(process.cwd(), '..', 'scripts', 'api', scriptName);
      
      const pythonProcess = spawn(this.pythonPath, [scriptPath, ...args]);
      
      let output = '';
      let error = '';

      pythonProcess.stdout.on('data', (data) => {
        output += data.toString();
      });

      pythonProcess.stderr.on('data', (data) => {
        error += data.toString();
      });

      pythonProcess.on('close', (code) => {
        if (code !== 0) {
          reject(new Error(`Python process failed: ${error}`));
        } else {
          resolve(output);
        }
      });
    });
  }
} 