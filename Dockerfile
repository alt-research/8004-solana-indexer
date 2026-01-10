# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./
COPY prisma ./prisma/

# Install dependencies
RUN npm ci

# Copy source
COPY tsconfig.json ./
COPY src ./src/
COPY idl ./idl/

# Generate Prisma client
RUN npx prisma generate

# Build TypeScript
RUN npm run build

# Production stage
FROM node:20-alpine AS runner

WORKDIR /app

# Copy package files and install production deps only
COPY package*.json ./
RUN npm ci --only=production

# Copy Prisma schema and generate client
COPY prisma ./prisma/
RUN npx prisma generate

# Copy built files
COPY --from=builder /app/dist ./dist/
COPY --from=builder /app/idl ./idl/

# Set environment
ENV NODE_ENV=production

# Run migrations and start
CMD ["sh", "-c", "npx prisma migrate deploy && node dist/index.js"]
