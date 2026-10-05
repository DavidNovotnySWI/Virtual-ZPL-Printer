/*
 *  This file is part of Virtual ZPL Printer.
 *
 *  Virtual ZPL Printer is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Virtual ZPL Printer is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Virtual ZPL Printer.  If not, see <https://www.gnu.org/licenses/>.
 */
using System.Net.Sockets;
using System.Security.RightsManagement;
using System.Text;
using System.Text.RegularExpressions;
using ImageCache.Abstractions;
using Labelary.Abstractions;
using Microsoft.Extensions.Logging;
using Prism.Events;
using VirtualPrinter.ApplicationSettings;
using VirtualPrinter.Db.Abstractions;
using VirtualPrinter.Handler.Abstractions;

namespace VirtualPrinter.HostedService.TcpSystem
{
	public class TcpListenerClientHandler(ILogger<TcpListenerClientHandler> logger, IEventAggregator eventAggregator, ISettings settings, IRequestHandlerFactory requestHandlerFactory, ILabelService labelService, IImageCacheRepository imageCacheRepository)
	{
		protected ILogger<TcpListenerClientHandler> Logger { get; set; } = logger;
		protected IEventAggregator EventAggregator { get; set; } = eventAggregator;
		protected ISettings Settings { get; set; } = settings;
		protected IRequestHandlerFactory RequestHandlerFactory { get; set; } = requestHandlerFactory;
		protected ILabelService LabelService { get; set; } = labelService;
		protected IImageCacheRepository ImageCacheRepository { get; set; } = imageCacheRepository;

		public event EventHandler OnCompleted = null;

		public async Task StartSessionAsync(TcpClient client, IPrinterConfiguration printerConfiguration, ILabelConfiguration labelConfiguration)
		{
			this.Logger.LogInformation("Handling incoming request from {endpoint}.", client.Client.LocalEndPoint);

			//
			// Set parameters.
			//
			client.ReceiveTimeout = this.Settings.ReceiveTimeout;
			client.SendTimeout = this.Settings.SendTimeout;
			client.NoDelay = this.Settings.NoDelay;
			client.ReceiveBufferSize = this.Settings.ReceiveBufferSize;
			client.SendBufferSize = this.Settings.SendBufferSize;
			client.LingerState = new LingerOption(this.Settings.Linger, this.Settings.LingerTime);

			//
			// Use user-specified encoding in order to display special characters correctly.
			//
			Encoding encoding = Encoding.UTF8;

			try
			{
				encoding = Encoding.GetEncoding(this.Settings.ReceivedDataEncoding);
				this.Logger.LogInformation("Using text encoding {encoding}.", encoding);
			}
			catch (Exception ex)
			{
				this.Logger.LogError(ex, "Exception while attempting to use encoding '{encoding}'. Falling back to UTF-8", this.Settings.ReceivedDataEncoding);
			}

			//
			// Get the network stream.
			//
			this.Logger.LogDebug("Getting the network stream for communications.");
			using (NetworkStream networkStream = client.GetStream())
			{
				bool closeConnection = false;

				while (client.Connected && networkStream.CanRead && !closeConnection)
				{
					using (MemoryStream ms = new())
					{
						bool graphicDownloadAcknowledged = false;
						this.Logger.LogInformation("The incoming connection is connected and can be read.");

						//
						// Set up a temporary buffer.
						//
						int bufferSize = client.ReceiveBufferSize == -1 ? 1024 : client.ReceiveBufferSize;
						this.Logger.LogDebug("Creating buffer of {size} bytes to read incoming data.", bufferSize);
						byte[] data = new byte[bufferSize];

						//
						// Create time stamp and cancellation token.
						//
						DateTime timestamp = DateTime.Now;
						using CancellationTokenSource tokenSource = new();

						while (!tokenSource.Token.IsCancellationRequested)
						{
							//
							// Read available data.
							//
							int numBytesRead = await networkStream.ReadAsync(data, tokenSource.Token);
							if (numBytesRead == 0)
							{
								this.Logger.LogInformation("The client closed the connection.");
								closeConnection = true;
								break;
							}

							string requestData = encoding.GetString(data, 0, numBytesRead);
							this.Logger.LogDebug("Data received: '{data}'.", requestData);

							//
							// Add the new data to the memory stream.
							//
							ms.Write(data, 0, numBytesRead);

							if (numBytesRead > 0)
							{
								//
								// Reset the timestamp if data has been received.
								//
								timestamp = DateTime.Now;

								//
								// Exit as soon as a complete ZPL job has been received so
								// that the label is processed without waiting for the
								// sender to close the connection. Only decode the newly
								// received chunk to avoid re-decoding the entire buffer
								// on every read.
								//
								string newChunk = encoding.GetString(data, 0, numBytesRead);

							// Zebra OPOS can poll status in the middle of a format. A real
							// printer answers that preparser command immediately, then keeps
							// accepting the unfinished ^XA ... ^XZ format. Do the same here.
							if (newChunk.Trim().Equals("~HS", StringComparison.OrdinalIgnoreCase))
							{
								ms.SetLength(ms.Length - numBytesRead);

								IRequestHandler statusHandler = await this.RequestHandlerFactory.GetHandlerAsync("~HS");
								(_, string statusResponse) = await statusHandler.HandleRequest(printerConfiguration, labelConfiguration, "~HS");

								if (statusResponse != null)
								{
									this.Logger.LogDebug("Sending in-stream ~HS response data: '{data}'.", statusResponse);
									await networkStream.WriteAsync(encoding.GetBytes(statusResponse));
									await networkStream.FlushAsync();
								}

								continue;
							}

							// ~DG is a complete command once its declared ASCII-hex payload
							// length has arrived; it is not terminated by ^XZ. Zebra's legacy
							// OPOS service object waits for XON before it sends the later ^XG
							// and MarkFeed format, so acknowledge the completed download.
							if (!graphicDownloadAcknowledged && IsCompleteGraphicDownload(encoding.GetString(ms.GetBuffer(), 0, (int)ms.Length)))
							{
								this.Logger.LogDebug("The ~DG graphic download is complete. Sending XON flow-control acknowledgement.");
								await networkStream.WriteAsync(new byte[] { 0x11 });
								await networkStream.FlushAsync();
								graphicDownloadAcknowledged = true;
							}

								if (newChunk.TrimEnd().EndsWith("^XZ", StringComparison.OrdinalIgnoreCase))
								{
									tokenSource.Cancel();
								}
							}
							this.Logger.LogInformation("{count} additional byte(s) were read from the incoming connection.", numBytesRead);
						}

					//
					// Only process the request if data was received.
					//
					if (ms.Length > 0)
					{
						//
						// Get the request data.
						//
						this.Logger.LogInformation("{count} byte(s) total were received.", ms.Length);
						string requestData = encoding.GetString(ms.ToArray(), 0, (int)ms.Length);
						this.Logger.LogDebug("Incoming data: '{data}'.", requestData);

						//
						// Get the request handler.
						//
						IRequestHandler requestHandler = await this.RequestHandlerFactory.GetHandlerAsync(requestData);
						this.Logger.LogDebug("Using request handler '{handler}' to handle the incoming request.", requestHandler.GetType().Name);

						//
						//  Call the handler.
						//
						(bool closeAfterRequest, string responseData) = (true, string.Empty);

						try
						{
							//
							// Get the handler for this request.
							//
							this.Logger.LogDebug("Calling {handler}.handleRequest().", requestHandler.GetType().Name);
							(closeAfterRequest, responseData) = await requestHandler.HandleRequest(printerConfiguration, labelConfiguration, requestData);

							//
							// If the handler provided a response, send it back.
							//
							if (responseData != null)
							{
								this.Logger.LogDebug("Sending response data: '{data}'.", responseData);
								byte[] buffer = encoding.GetBytes(responseData);
								await networkStream.WriteAsync(buffer);
								await networkStream.FlushAsync();
							}
							else
							{
								this.Logger.LogDebug("The handler did not return any response data.");
							}
						}
						catch (Exception ex)
						{
							this.Logger.LogError(ex, $"Exception occurred in {nameof(TcpListenerClientHandler)} while calling the request handler.");
						}
						closeConnection = closeAfterRequest;
						if (closeConnection)
						{
							this.Logger.LogInformation("Closing the client connection.");
						}
						else
						{
							this.Logger.LogInformation("Keeping the client connection open for the next request.");
						}
					}
					else
					{
						this.Logger.LogWarning("There was no data available on the incoming connection.");
					}
				}
			}
			}

			//
			// Fire the completion event.
			//
			this.OnCompleted?.Invoke(this, new EventArgs());
		}

		private static bool IsCompleteGraphicDownload(string requestData)
		{
			Match match = Regex.Match(requestData, @"~DG[^,]*,(?<bytes>\d+),\d+,", RegexOptions.IgnoreCase);
			if (!match.Success || !int.TryParse(match.Groups["bytes"].Value, out int expectedBytes))
			{
				return false;
			}

			string graphicData = requestData[(match.Index + match.Length)..];
			int hexCharacterCount = graphicData.Count(Uri.IsHexDigit);
			return hexCharacterCount >= expectedBytes * 2;
		}
	}
}
