"""Start metrics server for Prometheus monitoring."""

import os
import logging
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Prometheus metrics."""

    def do_GET(self):
        """Handle GET request."""
        if self.path == "/metrics":
            # Generate and send metrics
            try:
                metrics_output = generate_latest()
                self.send_response(200)
                self.send_header("Content-Type", CONTENT_TYPE_LATEST)
                self.end_headers()
                self.wfile.write(metrics_output)
                logger.debug("Metrics exposed successfully")
            except Exception as e:
                logger.error(f"Error generating metrics: {e}")
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"Internal Server Error")

        elif self.path == "/health":
            # Health check endpoint
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "healthy"}')
            logger.debug("Health check successful")

        else:
            # 404 for unknown paths
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        """Override to use custom logger."""
        logger.debug(format % args)


def start_metrics_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start the metrics HTTP server.

    Args:
        host: Host to bind to.
        port: Port to bind to.
    """
    server_address = (host, port)
    httpd = HTTPServer(server_address, MetricsHandler)

    logger.info(f"Starting metrics server on {host}:{port}")
    logger.info(f"Metrics available at http://{host}:{port}/metrics")
    logger.info(f"Health check available at http://{host}:{port}/health")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down metrics server")
        httpd.shutdown()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Start Prometheus metrics server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (default: INFO)")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.loglevel),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Start server
    start_metrics_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
