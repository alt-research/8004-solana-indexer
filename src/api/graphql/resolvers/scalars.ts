import { GraphQLScalarType, Kind } from 'graphql';

export const BigIntScalar = new GraphQLScalarType({
  name: 'BigInt',
  description: 'BigInt scalar — serialized as string to preserve precision beyond Number.MAX_SAFE_INTEGER',
  serialize(value: unknown): string {
    if (typeof value === 'bigint') return value.toString();
    if (typeof value === 'number') return String(value);
    if (typeof value === 'string') return value;
    throw new TypeError(`BigInt cannot represent value: ${value}`);
  },
  parseValue(value: unknown): bigint {
    if (typeof value === 'string') return BigInt(value);
    if (typeof value === 'number') return BigInt(value);
    if (typeof value === 'bigint') return value;
    throw new TypeError(`BigInt cannot represent value: ${value}`);
  },
  parseLiteral(ast): bigint {
    if (ast.kind === Kind.INT || ast.kind === Kind.STRING) {
      return BigInt(ast.value);
    }
    throw new TypeError(`BigInt cannot represent literal: ${ast.kind}`);
  },
});

export const BigDecimalScalar = new GraphQLScalarType({
  name: 'BigDecimal',
  description: 'BigDecimal scalar — serialized as string for arbitrary precision decimals',
  serialize(value: unknown): string {
    if (typeof value === 'string') return value;
    if (typeof value === 'number') return String(value);
    if (typeof value === 'bigint') return value.toString();
    throw new TypeError(`BigDecimal cannot represent value: ${value}`);
  },
  parseValue(value: unknown): string {
    if (typeof value === 'string') return value;
    if (typeof value === 'number') return String(value);
    throw new TypeError(`BigDecimal cannot represent value: ${value}`);
  },
  parseLiteral(ast): string {
    if (ast.kind === Kind.STRING || ast.kind === Kind.INT || ast.kind === Kind.FLOAT) {
      return ast.value;
    }
    throw new TypeError(`BigDecimal cannot represent literal: ${ast.kind}`);
  },
});

export const BytesScalar = new GraphQLScalarType({
  name: 'Bytes',
  description: 'Bytes scalar — hex-encoded byte string',
  serialize(value: unknown): string {
    if (value instanceof Buffer) return value.toString('hex');
    if (value instanceof Uint8Array) return Buffer.from(value).toString('hex');
    if (typeof value === 'string') return value;
    throw new TypeError(`Bytes cannot represent value: ${value}`);
  },
  parseValue(value: unknown): string {
    if (typeof value === 'string') return value;
    throw new TypeError(`Bytes cannot represent value: ${value}`);
  },
  parseLiteral(ast): string {
    if (ast.kind === Kind.STRING) return ast.value;
    throw new TypeError(`Bytes cannot represent literal: ${ast.kind}`);
  },
});

export const scalarResolvers = {
  BigInt: BigIntScalar,
  BigDecimal: BigDecimalScalar,
  Bytes: BytesScalar,
};
