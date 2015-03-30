from server import Cluster;

def main():
    cluster = Cluster.Cluster() ;
    
    print('All Servers')
    print('------------------------------------------');

    cluster.report();
    
    print('Kafka Server Set')
    print('------------------------------------------');
    cluster.getServersByRole('kafka').report() ;

if __name__ == '__main__':
    main()
