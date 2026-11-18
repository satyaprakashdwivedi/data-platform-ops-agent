// Prints the n8n credentials as JSON for `n8n import:credentials`, from environment variables.
// IDs match the ones referenced by n8n/build_workflows.py.
const env = (name, fallback) => {
  const value = process.env[name] ?? fallback;
  if (value === undefined || value === '') throw new Error(`${name} must be set`);
  return value;
};

const credentials = [
  {
    id: 'opsAnthropic0001', name: 'Anthropic', type: 'anthropicApi',
    data: { apiKey: env('ANTHROPIC_API_KEY'), url: env('ANTHROPIC_BASE_URL', 'https://api.anthropic.com') },
  },
  { id: 'opsToolsBearer01', name: 'Ops tools token', type: 'httpBearerAuth', data: { token: env('OPS_TOOLS_TOKEN') } },
  {
    id: 'opsWebhookToken1', name: 'Webhook token', type: 'httpHeaderAuth',
    data: { name: 'X-Webhook-Token', value: env('N8N_WEBHOOK_TOKEN') },
  },
  {
    id: 'opsApprovers0001', name: 'Approvers', type: 'httpBasicAuth',
    data: { user: env('APPROVER_USER'), password: env('APPROVER_PASSWORD') },
  },
];

process.stdout.write(JSON.stringify(credentials));
