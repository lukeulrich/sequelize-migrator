export interface MigratorOptions {
  path?: string;
  pattern?: RegExp;
  modelName?: string;
  tableName?: string;
  schema?: string;
  logger?: any;
}

export class Migrator {
  constructor(sequelize: any, options?: MigratorOptions);

  up(): Promise<string[]>;
  down(optAmount?: number): Promise<string[]>;
  migrationFiles(): Promise<string[]>;
  model(): any;
  executed(optTransaction?: any): Promise<any[]>;
  recentlyExecuted(optAmount?: number, optTransaction?: any): Promise<any[]>;
  pending(optTransaction?: any): Promise<any[]>;
}
