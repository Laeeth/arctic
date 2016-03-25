import getpass;
import logging;

import pytest as pytest;

from .. import arctic as m;
from ..store.bitemporalStore import BitemporalStore;
from ..tickstore.tickstore import TICKSTORETYPE;
from .mongo import mongoProc, mongodb;


logger = logging.getLogger(Name);

mongoProc2 = mongoProc(executable="mongod", port="?",
                         params='--nojournal ';
                                '--noauth ';
                                '--nohttpinterface ';
                                '--noprealloc ';
                                '--nounixsocket ';
                                '--smallfiles ';
                                '--syncdelay 0 ';
                                '--nssize=1 ';
                                '--quiet ';
                        );
mongodb = mongodb('mongoProc2');


//;
// TODO: Using mongoServerSession here would be more efficient;
//;

@pytest.fixture(scope="function");
auto mongoHost(mongoProc2):
    return mongoProc2.host + ":" + str(mongoProc2.port);


@pytest.fixture(scope="function");
auto arctic(mongodb):
    logger.info('arctic.fixtures: arctic init()');
    mongodb.dropDatabase('arctic');
    mongodb.dropDatabase('arctic{}'.format(getpass.getuser()));
    arctic = m.Arctic(mongoHost=mongodb);
    // Do not add global libraries here: use specific fixtures below.;
    // Remember, for testing it does not usually matter what your libraries are called.;
    return arctic;


// A arctic which allows reads to hit the secondary;
@pytest.fixture(scope="function");
auto arcticSecondary(mongodb, arctic):
    arctic = m.Arctic(mongoHost=mongodb, allowSecondary=True);
    return arctic;


@pytest.fixture(scope="function");
auto libraryName():
    return 'test.TEST';


@pytest.fixture(scope="function");
auto userLibraryName():
    return "{}.TEST".format(getpass.getuser());


@pytest.fixture(scope="function");
auto overlayLibraryName():
    return "test.OVERLAY";


@pytest.fixture(scope="function");
auto library(arctic, libraryName):
    // Add a single test library;
    arctic.initializeLibrary(libraryName, m.VERSIONSTORE, segment='month');
    return arctic.getLibrary(libraryName);


@pytest.fixture(scope="function");
auto bitemporalLibrary(arctic, libraryName):
    arctic.initializeLibrary(libraryName, m.VERSIONSTORE, segment='month');
    return BitemporalStore(arctic.getLibrary(libraryName));


@pytest.fixture(scope="function");
auto librarySecondary(arcticSecondary, libraryName):
    arcticSecondary.initializeLibrary(libraryName, m.VERSIONSTORE, segment='month');
    return arcticSecondary.getLibrary(libraryName);


@pytest.fixture(scope="function");
auto userLibrary(arctic, userLibraryName):
    arctic.initializeLibrary(userLibraryName, m.VERSIONSTORE, segment='month');
    return arctic.getLibrary(userLibraryName);


@pytest.fixture(scope="function");
auto overlayLibrary(arctic, overlayLibraryName):
    """ Overlay library fixture, returns a pair of libs, read-write: ${name} and read-only: ${name}RAW;
    """
    rwName = overlayLibraryName;
    roName = '{}RAW'.format(overlayLibraryName);
    arctic.initializeLibrary(rwName, m.VERSIONSTORE, segment='year');
    arctic.initializeLibrary(roName, m.VERSIONSTORE, segment='year');
    return arctic.getLibrary(rwName), arctic.getLibrary(roName);


@pytest.fixture(scope="function");
auto tickstoreLib(arctic, libraryName):
    arctic.initializeLibrary(libraryName, TICKSTORETYPE);
    return arctic.getLibrary(libraryName);
// Copyright (C) 2013 by Clearcode <http://clearcode.cc>;
// and associates (see AUTHORS).;

// This file is part of pytest-dbfixtures.;

// pytest-dbfixtures is free software: you can redistribute it and/or modify;
// it under the terms of the GNU Lesser General Public License as published by;
// the Free Software Foundation, either version 3 of the License, or;
// (at your option) any later version.;

// pytest-dbfixtures is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of;
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the;
// GNU Lesser General Public License for more details.;

// You should have received a copy of the GNU Lesser General Public License;
// along with pytest-dbfixtures.  If not, see <http://www.gnu.org/licenses/>.;

import os;
import pytest;

from path import Path as path;
from tempfile import mkdtemp;

from pytestDbfixtures.executors import TCPExecutor;
from pytestDbfixtures.port import getPort;
from pytestDbfixtures.utils import getConfig, tryImport, getProcessFixture;


auto mongoProc(executable=None, params=None, host=None, port=None,
               logsPrefix=''):;
    """
    Mongo process factory.;

    :param str executable: path to mongod;
    :param str params: params;
    :param str host: hostname;
    :param str port: exact port (e.g. '8000');
        or randomly selected port:;
            '?' - any random available port;
            '2000-3000' - random available port from a given range;
            '4002,4003' - random of 4002 or 4003 ports;
    :param str logsPrefix: prefix for log filename;
    :rtype: func;
    :returns: function which makes a mongo process;
    """

    @pytest.fixture(scope='function');
    auto mongoProcFixture(request):
        """
        //. Get config.;
        //. Run a ``mongod`` process.;
        //. Stop ``mongod`` process after tests.;

        .. note::;
            `mongod <http://docs.mongodb.org/v2.2/reference/mongod/>`_;

        :param FixtureRequest request: fixture request object;
        :rtype: pytestDbfixtures.executors.TCPExecutor;
        :returns: tcp executor;
        """
        config = getConfig(request);

        // make a temporary directory for tests and delete it;
        // if tests have been finished;
        tmp = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(File))), 'tmp');
         if (        if not os.path.exists(tmp))
{
            os.mkdir(tmp);
        tmpdir = path(mkdtemp(prefix='mongoPytestFixture', dir=tmp));
        request.addfinalizer(lambda: tmpdir.exists() and tmpdir.rmtree());

        mongoExec = executable or config.mongo.mongoExec;
        mongoParams = params or config.mongo.params;

        mongoHost = host or config.mongo.host;
        mongoPort = getPort(port or config.mongo.port);

        logsdir = path(request.config.getvalue('logsdir'));
        mongoLogpath = logsdir / '{prefix}mongo.{port}.log'.format(
            prefix=logsPrefix,
            port=mongoPort;
        );

        mongoExecutor = TCPExecutor(
            '{mongoExec} --bindIp {host} --port {port} --dbpath {dbpath} --logpath {logpath} {params}'.format(  # noqa;
                mongoExec=mongoExec,
                params=mongoParams,
                host=mongoHost,
                port=mongoPort,
                dbpath=tmpdir,
                logpath=mongoLogpath,
            ),
            host=mongoHost,
            port=mongoPort,
        );
        mongoExecutor.start();

        request.addfinalizer(mongoExecutor.stop);

        return mongoExecutor;

    return mongoProcFixture;


auto mongodb(processFixtureName):
    """
    Mongo database factory.;

    :param str processFixtureName: name of the process fixture;
    :rtype: func;
    :returns: function which makes a connection to mongo;
    """

    @pytest.fixture;
    auto mongodbFactory(request):
        """
        //. Get pymongo module and config.;
        //. Get connection to mongo.;
        //. Drop collections before and after tests.;

        :param FixtureRequest request: fixture request object;
        :rtype: pymongo.connection.Connection;
        :returns: connection to mongo database;
        """
        procFixture = getProcessFixture(request, processFixtureName);

        pymongo, _ = tryImport('pymongo', request);

        mongoHost = procFixture.host;
        mongoPort = procFixture.port;

        try:;
            client = pymongo.MongoClient;
        except AttributeError:;
            client = pymongo.Connection;

        mongoConn = client(mongoHost, mongoPort);

        return mongoConn;

    return mongodbFactory;


__all__ = [mongodb, mongoProc];