%define name logmetrics_collector
%define tester_name logmetrics_parsertest
%define path /usr/local
%define version 0.4
%define release 9
%define app_path src/github.com/mathpl/logmetrics
%define pcre_version 8.32
 
Name:           %{name}
Version:        %{version}
Release:        %{release}
Summary:        Log file metrics collector and statistical aggregator for OpenTSDB
Group:          System/Monitoring
License:        GPL
Source0:        /source/%{name}/%{name}-%{version}.src.tgz
Source1:        /source/%{name}/pcre-8.32.tar.gz
Source2:        /source/%{name}
Requires:       tcollector
BuildRequires:  go-devel = 1.2
BuildRoot:      /build/%{name}-%{version}-%{release}
AutoReqProv:    no
 
%description
Parse log files containing performance data, computes statistics and outputs them
to TSD or tcollector while using limited ressources.
 
%prep
rm -rf $RPM_BUILD_DIR/%{name}-%{version}-%{release}
mkdir -p $RPM_BUILD_DIR/%{name}-%{version}-%{release}
tar xvzf %{SOURCE0} -C $RPM_BUILD_DIR/%{name}-%{version}-%{release}
tar xvzf %{SOURCE1} -C $RPM_BUILD_DIR/
cp %{SOURCE2} $RPM_BUILD_DIR/%{name}-%{version}-%{release}/

#%post
#if [ "$1" = 1 ]; then
#  chkconfig --add logmetrics_collector
#  chkconfig logmetrics_collector on
#  service logmetrics_collector start
#fi

#%preun
#if [ "$1" = 0 ]; then
#  service logmetrics_collector stop
#  chkconfig logmetrics_collector off
#  chkconfig --del logmetrics_collector
#fi
 
%build
#First build pcre to enable static linking
cd $RPM_BUILD_DIR/pcre-%{pcre_version}/
CFLAGS="-fPIC" CXXFLAGS="-fPIC" ./configure 
make

export GOPATH=$RPM_BUILD_DIR/%{name}-%{version}-%{release} \
       GOROOT=/usr/local/go

cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/%{app_path}/main
cp $RPM_BUILD_DIR/pcre-%{pcre_version}/.libs/*.a .
CGO_LDFLAGS="-lpcre -L`pwd`" CGO_CFLAGS="-I$RPM_BUILD_DIR/pcre-%{pcre_version}/" \
  /usr/local/go/bin/go build -o %{name} logmetrics_collector.go

cd $RPM_BUILD_DIR/%{name}-%{version}-%{release}/%{app_path}/parsertest
cp $RPM_BUILD_DIR/pcre-%{pcre_version}/.libs/*.a .
CGO_LDFLAGS="-lpcre -L`pwd`" CGO_CFLAGS="-I$RPM_BUILD_DIR/pcre-%{pcre_version}/" \
  /usr/local/go/bin/go build -o %{tester_name} %{tester_name}.go
 
%install
%{__mkdir_p} ${RPM_BUILD_ROOT}/usr/local/bin/
%{__cp} $RPM_BUILD_DIR/%{name}-%{version}-%{release}/%{app_path}/main/%{name} ${RPM_BUILD_ROOT}/usr/local/bin/
%{__cp} $RPM_BUILD_DIR/%{name}-%{version}-%{release}/%{app_path}/parsertest/%{tester_name} ${RPM_BUILD_ROOT}/usr/local/bin/
%{__mkdir_p} ${RPM_BUILD_ROOT}/etc/init.d/
%{__cp} $RPM_BUILD_DIR/%{name}-%{version}-%{release}/%{name} ${RPM_BUILD_ROOT}/etc/init.d/
 
%files
%defattr(0755,root,root,-)
/usr/local/bin/%{name}
/usr/local/bin/%{tester_name}
/etc/init.d/%{name}

%changelog
* Mon Jul 28 2014 Mathieu Payeur <mathpl@github.com> - 0.4-9
- Better initscript.

* Tue May 09 2014 Mathieu Payeur <mathpl@github.com> - 0.4-8
- Logging timestamps with stale logging.

* Tue May 09 2014 Mathieu Payeur <mathpl@github.com> - 0.4-7
- Better name to support to print staled metrics.

* Tue May 09 2014 Mathieu Payeur <mathpl@github.com> - 0.4-6
- Support to print staled metrics.

* Tue May 09 2014 Mathieu Payeur <mathpl@github.com> - 0.4-5
- Logic error with stale removal. Yet again!

* Tue May 09 2014 Mathieu Payeur <mathpl@github.com> - 0.4-4
- Logic error with stale removal. Again!

* Tue May 08 2014 Mathieu Payeur <mathpl@github.com> - 0.4-3
- Logic error with stale removal.

* Tue May 08 2014 Mathieu Payeur <mathpl@github.com> - 0.4-2
- Better config options for stale metrics. Sending duplicate metrics now an option.

* Tue May 07 2014 Mathieu Payeur <mathpl@github.com> - 0.4-1
- Stale value support for realtime metrics.

* Tue Apr 15 2014 Mathieu Payeur <mathpl@github.com> - 0.3-10
- Fix for duplicate keys sent.

* Tue Apr 15 2014 Mathieu Payeur <mathpl@github.com> - 0.3-9
- Fix for EWMA stale metric mechanic.

* Tue Apr 15 2014 Mathieu Payeur <mathpl@github.com> - 0.3-8
- EWMA tuning phase 2.

* Tue Apr 14 2014 Mathieu Payeur <mathpl@github.com> - 0.3-7
- EWMA tuning.

* Tue Apr 14 2014 Mathieu Payeur <mathpl@github.com> - 0.3-6
- Configuration stale threshold for metrics.

* Tue Apr 14 2014 Mathieu Payeur <mathpl@github.com> - 0.3-5
- Internals stats output more often, inotify/polling configurable.

* Tue Apr 11 2014 Mathieu Payeur <mathpl@github.com> - 0.3-4
- Fixes for inotify.

* Tue Apr 11 2014 Mathieu Payeur <mathpl@github.com> - 0.3-3
- Now pushes internal processing stats to TSD.

* Tue Apr 10 2014 Mathieu Payeur <mathpl@github.com> - 0.3-2
- pcre now statically linked.

* Tue Apr 9 2014 Mathieu Payeur <mathpl@github.com> - 0.3-1
- Replacing native go regex by pcre bindings.

* Tue Apr 02 2014 Mathieu Payeur <mathpl@github.com> - 0.2-17
- fix in init script to properly drop privileges.

* Tue Apr 02 2014 Mathieu Payeur <mathpl@github.com> - 0.2-16
- Removing CentOS 5.8 req.

* Tue Apr 02 2014 Mathieu Payeur <mathpl@github.com> - 0.2-15
- Config parameter name change

* Tue Apr 02 2014 Mathieu Payeur <mathpl@github.com> - 0.2-14
- Option for time out of order warnings.

* Tue Mar 31 2014 Mathieu Payeur <mathpl@github.com> - 0.2-13
- Left debugging things in.

* Tue Mar 31 2014 Mathieu Payeur <mathpl@github.com> - 0.2-11
- Slight mistake in setuid script.

* Tue Mar 31 2014 Mathieu Payeur <mathpl@github.com> - 0.2-10
- Better init script.

* Tue Mar 31 2014 Mathieu Payeur <mathpl@github.com> - 0.2-9
- Cleanup of user setuid... no more.

* Tue Mar 31 2014 Mathieu Payeur <mathpl@github.com> - 0.2-8
- EWMA tuning + support for multiple value from single math group.

* Tue Mar 26 2014 Mathieu Payeur <mathpl@github.com> - 0.2-7
- Bugfix on EWMA tuning.

* Tue Mar 26 2014 Mathieu Payeur <mathpl@github.com> - 0.2-6
- New paramters for EWMA generation tuning.

* Tue Mar 26 2014 Mathieu Payeur <mathpl@github.com> - 0.2-5
- Another EWMA refresh bugfix.

* Tue Mar 26 2014 Mathieu Payeur <mathpl@github.com> - 0.2-3
- EWMA refresh bugfix.

* Tue Mar 26 2014 Mathieu Payeur <mathpl@github.com> - 0.2-1
- Float support.

* Tue Mar 24 2014 Mathieu Payeur <mathpl@github.com> - 0.1-4
- Now named logmetrics_collector

* Tue Mar 24 2014 Mathieu Payeur <mathpl@github.com> - 0.1-3
- better init script.

* Tue Mar 24 2014 Mathieu Payeur <mathpl@github.com> - 0.1-2
- A few fixes.

* Tue Mar 24 2014 Mathieu Payeur <mathpl@github.com> - 0.1-1
- Initial specfile.

