Name:           taskworker-core
Version:        0.4.1
Release:        1%{?dist}
Summary:        Taskworker middleware platform 

License:        Apache 2.0 
URL:            https://distrinet.cs.kuleuven.be/software/DREAMaaS
Source0:        https://distrinet.cs.kuleuven.be/software/DREAMaaS/taskworker-core.tar.bz2

BuildArch:      noarch
BuildRequires:  maven
BuildRequires:  systemd
Requires:       java

%description


%prep
%setup -q -n taskworker-core


%build
mvn package

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/usr/share/java
install -p -m 644 target/taskworker-core.jar $RPM_BUILD_ROOT/usr/share/java/taskworker-core.jar
mkdir -p $RPM_BUILD_ROOT/usr/bin
install -p -m 755 bin/taskworker-client $RPM_BUILD_ROOT/usr/bin
install -p -m 755 bin/taskworker-server $RPM_BUILD_ROOT/usr/bin

mkdir -p $RPM_BUILD_ROOT/etc/taskworker
install -p -m 644 src/main/resources/config.properties $RPM_BUILD_ROOT/etc/taskworker/

mkdir -p $RPM_BUILD_ROOT/usr/share/doc/taskworker
install Readme.md $RPM_BUILD_ROOT/usr/share/doc/taskworker

mkdir -p $RPM_BUILD_ROOT/lib/systemd/system
install -p taskworker-server.service $RPM_BUILD_ROOT/lib/systemd/system

%files
%doc /usr/share/doc/taskworker/*
/usr/share/java/*
/usr/bin/*
%config /etc/taskworker/*
/lib/systemd/system/*

%changelog
