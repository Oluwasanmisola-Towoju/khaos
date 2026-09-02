<?php

declare(strict_types=1);

final class KhaosDispatchException extends \RuntimeException {}

final class KhaosDispatcher
{
    private array $pending = [];

    private readonly string $apiUrl;

    public function __construct(
        string $apiUrl,
        private readonly int $timeoutSeconds = 5,
        private readonly int $connectTimeoutSeconds = 3
    ) {
        $trimmed = rtrim($apiUrl, '/');
        if ($trimmed === '') {
            throw new \InvalidArgumentException('apiUrl must not be empty');
        }
        $this->apiUrl = $trimmed;
    }

    public function addRiderUpdate(
        string $riderId,
        string $orderId,
        float $lat,
        float $lng,
        string $status,
        int $eta
    ): void {
        if (trim($riderId) === '') {
            throw new \InvalidArgumentException('riderId must not be empty');
        }
        if (trim($orderId) === '') {
            throw new \InvalidArgumentException('orderId must not be empty');
        }
        if ($lat < -90.0 || $lat > 90.0) {
            throw new \InvalidArgumentException("lat must be between -90 and 90, got {$lat}");
        }
        if ($lng < -180.0 || $lng > 180.0) {
            throw new \InvalidArgumentException("lng must be between -180 and 180, got {$lng}");
        }
        if (trim($status) === '') {
            throw new \InvalidArgumentException('status must not be empty');
        }
        if ($eta < 0) {
            throw new \InvalidArgumentException("eta must be >= 0, got {$eta}");
        }

        $this->pending[] = [
            'riderId' => $riderId,
            'orderId' => $orderId,
            'lat'     => $lat,
            'lng'     => $lng,
            'status'  => $status,
            'eta'     => $eta
        ];
    }

    public function pendingCount(): int
    {
        return count($this->pending);
    }

    public function dispatchBatch(): array
    {
        if ($this->pending === []) {
            return [];
        }

        $requestItems = [];
        foreach ($this->pending as $seqId => $update) {
            $requestItems[] = [
                'seq_id'           => $seqId,
                'operation_type'   => 'WRITE',
                'rider_id'         => $update['riderId'],
                'payload'          => [
                    'order_id'       => $update['orderId'],
                    'latitude'       => $update['lat'],
                    'longitude'      => $update['lng'],
                    'current_status' => $update['status'],
                    'eta_minutes'    => $update['eta']
                ]
            ];
        }

        try {
            $requestBody = json_encode($requestItems, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new KhaosDispatchException(
                'Failed to encode outgoing batch as JSON: ' . $e->getMessage(),
                previous: $e
            );
        }

        $responseBody = $this->sendRequest($requestBody);
        $decoded = $this->decodeResponse($responseBody);

        $this->pending = [];

        return $decoded;
    }

    private function sendRequest(string $requestBody): string
    {
        $ch = curl_init();
        if ($ch === false) {
            throw new KhaosDispatchException('Failed to initialize cURL handle');
        }

        curl_setopt_array($ch, [
            CURLOPT_URL            => $this->apiUrl . '/api/v1/riders/batch',
            CURLOPT_POST           => true,
            CURLOPT_POSTFIELDS     => $requestBody,
            CURLOPT_HTTPHEADER     => [
                'Content-Type: application/json',
                'Accept: application/json',
            ],
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT        => $this->timeoutSeconds,
            CURLOPT_CONNECTTIMEOUT => $this->connectTimeoutSeconds,
        ]);

        $responseBody = curl_exec($ch);
        $curlErrno    = curl_errno($ch);
        $curlError    = curl_error($ch);
        $httpCode     = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        curl_close($ch);

        // CURLE_OPERATION_TIMEDOUT a timeout and a connection refusal are
        // both "the gateway did not give us a usable response," and the
        // caller's remediation (retry, alert, back off) is the same
        // either way, so they share one exception type rather than being
        // split into a separate timeout-specific exception.

        if ($curlErrno !== 0) {
            throw new KhaosDispatchException(
                sprintf('cURL transport error (errno %d): %s', $curlErrno, $curlError)
            );
        }

        if ($httpCode !== 200) {
            $snippet = is_string($responseBody) && $responseBody !== ''
                ? substr($responseBody, 0, 500)
                : '(empty body)';
            throw new KhaosDispatchException(
                sprintf('Khaos gateway returned HTTP %d: %s', $httpCode, $snippet)
            );
        }

        if (!is_string($responseBody)) {
            // Structurally unreachable when CURLOPT_RETURNTRANSFER is true and curl_errno is 0, but guarded explicitly rather
            // than assumed, since dereferencing a bool as a string below would otherwise fail in a confusing way.
            throw new KhaosDispatchException('Khaos gateway returned an empty or invalid response body');
        }

        return $responseBody;
    }

    private function decodeResponse(string $responseBody): array
    {
        try {
            $decoded = json_decode($responseBody, true, 512, JSON_THROW_ON_ERROR);
        } catch (\JsonException $e) {
            throw new KhaosDispatchException(
                'Failed to decode Khaos gateway response as JSON: ' . $e->getMessage(),
                previous: $e,
            );
        }

        if (!is_array($decoded)) {
            throw new KhaosDispatchException(
                'Unexpected response shape from Khaos gateway: expected a JSON array, got ' . get_debug_type($decoded)
            );
        }

        return $decoded;
    }
}






// DEMONSTRATION SCRIPT
if (realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    $apiUrl = getenv('KHAOS_API_URL') ?: 'http://127.0.0.1:8080';

    $dispatcher = new KhaosDispatcher($apiUrl);

    $dispatcher->addRiderUpdate(
        riderId: '11111111-1111-1111-1111-111111111111',
        orderId: '22222222-2222-2222-2222-222222222222',
        lat: 6.5244,
        lng: 3.3792,
        status: 'EN_ROUTE',
        eta: 12,
    );

    echo "Dispatching {$dispatcher->pendingCount()} rider updates to {$apiUrl}...\n";

    try {
        $results = $dispatcher->dispatchBatch();
        echo "Success. Gateway returned " . count($results) . " result(s):\n";
        foreach ($results as $result) {
            echo '  ' . json_encode($result) . "\n";
        }
    } catch (KhaosDispatchException $e) {
        fwrite(STDERR, "Dispatch failed: {$e->getMessage()}\n");
        exit(1);
    }
}