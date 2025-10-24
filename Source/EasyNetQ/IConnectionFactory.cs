using RabbitMQ.Client;
using System.Security.Authentication;

namespace EasyNetQ
{
    public interface IConnectionFactory
    {
        IConnection CreateConnection();
        ConnectionConfiguration Configuration { get; }
        HostConfiguration CurrentHost { get; }
        bool Next();
        void Success();
        void Reset();
        bool Succeeded { get; }
    }

    public class ConnectionFactoryWrapper : IConnectionFactory
    {
        public virtual ConnectionConfiguration Configuration { get; }
        private readonly IClusterHostSelectionStrategy<ConnectionFactoryInfo> clusterHostSelectionStrategy;

        public ConnectionFactoryWrapper(ConnectionConfiguration connectionConfiguration, IClusterHostSelectionStrategy<ConnectionFactoryInfo> clusterHostSelectionStrategy)
        {
            this.clusterHostSelectionStrategy = clusterHostSelectionStrategy;

            Preconditions.CheckNotNull(connectionConfiguration, "connectionConfiguration");
            Preconditions.CheckAny(connectionConfiguration.Hosts, "connectionConfiguration", "At least one host must be defined in connectionConfiguration");

            Configuration = connectionConfiguration;

            foreach (var hostConfiguration in Configuration.Hosts)
            {
                var connectionFactory = new ConnectionFactory
                {
                    UseBackgroundThreadsForIO = connectionConfiguration.UseBackgroundThreads,
                    AutomaticRecoveryEnabled = false,
                    TopologyRecoveryEnabled = false
                };

                if (connectionConfiguration.AMQPConnectionString != null)
                {
                    connectionFactory.Uri = connectionConfiguration.AMQPConnectionString;
                }

                connectionFactory.HostName = hostConfiguration.Host;

                if (connectionFactory.VirtualHost == "/")
                    connectionFactory.VirtualHost = Configuration.VirtualHost;

                if (connectionFactory.UserName == "guest")
                    connectionFactory.UserName = Configuration.UserName;

                if (connectionFactory.Password == "guest")
                    connectionFactory.Password = Configuration.Password;

                if (connectionFactory.Port == -1)
                    connectionFactory.Port = hostConfiguration.Port;

                if (hostConfiguration.Ssl.Enabled)
                    connectionFactory.Ssl = hostConfiguration.Ssl;

                //Prefer SSL configurations per each host but fall back to ConnectionConfiguration's SSL configuration for backwards compatibility
                else if (connectionConfiguration.Ssl.Enabled)
                    connectionFactory.Ssl = UpdateHostConfigurationSslOptionsWithConnectionConfiguration(connectionConfiguration, hostConfiguration);

                connectionFactory.RequestedHeartbeat = Configuration.RequestedHeartbeat;
                connectionFactory.ClientProperties = Configuration.ClientProperties;
                connectionFactory.AuthMechanisms = Configuration.AuthMechanisms;
                clusterHostSelectionStrategy.Add(new ConnectionFactoryInfo(connectionFactory, hostConfiguration));
            }
        }

        private static SslOption UpdateHostConfigurationSslOptionsWithConnectionConfiguration(ConnectionConfiguration configuration, HostConfiguration hostConfiguration)
        {
            var sslOption = new SslOption()
            {
                AcceptablePolicyErrors = hostConfiguration.Ssl.AcceptablePolicyErrors,
                CertPassphrase = hostConfiguration.Ssl.CertPassphrase,
                CertPath = hostConfiguration.Ssl.CertPath,
                CertificateSelectionCallback = hostConfiguration.Ssl.CertificateSelectionCallback,
                CertificateValidationCallback = hostConfiguration.Ssl.CertificateValidationCallback,
                Certs = hostConfiguration.Ssl.Certs,
                Enabled = true,
                Version = hostConfiguration.Ssl.Version != SslProtocols.None ?  hostConfiguration.Ssl.Version : (configuration.Ssl.Version != SslProtocols.None ? configuration.Ssl.Version : SslProtocols.Default),
                ServerName = !string.IsNullOrEmpty(hostConfiguration.Ssl.ServerName) ? hostConfiguration.Ssl.ServerName : (!string.IsNullOrEmpty(configuration.Ssl.ServerName) ? configuration.Ssl.ServerName : hostConfiguration.Host)

            };

            return sslOption;
        }

        public virtual IConnection CreateConnection()
        {
            object connectionNameValue = null;
            Configuration?.ClientProperties.TryGetValue("connection_name", out connectionNameValue);
            return clusterHostSelectionStrategy.Current().ConnectionFactory.CreateConnection(connectionNameValue as string);
        }

        public virtual HostConfiguration CurrentHost => clusterHostSelectionStrategy.Current().HostConfiguration;

        public virtual bool Next()
        {
            return clusterHostSelectionStrategy.Next();
        }

        public virtual void Reset()
        {
            clusterHostSelectionStrategy.Reset();
        }

        public virtual void Success()
        {
            clusterHostSelectionStrategy.Success();
        }

        public virtual bool Succeeded => clusterHostSelectionStrategy.Succeeded;
    }

    public class ConnectionFactoryInfo
    {
        public ConnectionFactoryInfo(ConnectionFactory connectionFactory, HostConfiguration hostConfiguration)
        {
            ConnectionFactory = connectionFactory;
            HostConfiguration = hostConfiguration;
        }

        public ConnectionFactory ConnectionFactory { get; private set; }
        public HostConfiguration HostConfiguration { get; private set; }
    }

}
